#include "event_groups.h"

// --- Event Group Definitions ---
#define EVENT_SD_MOUNTED    (1 << 0)
#define EVENT_FILES_OPEN    (1 << 1)
#define EVENT_LOG_TRIGGER   (1 << 2)

// --- Handles ---
EventGroupHandle_t xSDStateEventGroup;
QueueHandle_t g_sd_data_queue;
TimerHandle_t xLogTimerHandle;

// --- Function Prototypes ---
void vLogTimerCallback(TimerHandle_t xTimer);
void vSDMaintenanceTask(void *pvParameters);
void vStorageTask(void *pvParameters);

// --- Initialization Wrapper (Call in main() before scheduler) ---
void Init_SD_System(void) {
    xSDStateEventGroup = xEventGroupCreate();
    // Queue depth of 20 allows buffering up to 100 seconds of data if SD is slow
    g_sd_data_queue = xQueueCreate(20, sizeof(master_log_payload_t));
    
    // Auto-reload timer for exactly 5 seconds
    xLogTimerHandle = xTimerCreate("LogTimer", pdMS_TO_TICKS(5000), pdTRUE, (void*)0, vLogTimerCallback);
    xTimerStart(xLogTimerHandle, 0);
}







void vLogTimerCallback(TimerHandle_t xTimer) {
    // Fire the trigger event bit. This is ISR safe logic inside a timer task.
    if(xSDStateEventGroup != NULL) {
        xEventGroupSetBits(xSDStateEventGroup, EVENT_LOG_TRIGGER);
    }
}









void vSDMaintenanceTask(void *pvParameters) {
    // Initial startup delay
    vTaskDelay(pdMS_TO_TICKS(2000));
    
    for(;;) {
        // Wait until the system is NOT ready (Bits 0 or 1 are missing).
        // xEventGroupWaitBits isn't ideal for "wait for bit to be 0".
        // Instead, we check the state periodically or when signalled by failure.
        
        EventBits_t currentBits = xEventGroupGetBits(xSDStateEventGroup);
        
        // If files are NOT open, we need to do work.
        if ((currentBits & EVENT_FILES_OPEN) == 0) {
            
            // Step A: Mount SD Card (if not mounted)
            if ((currentBits & EVENT_SD_MOUNTED) == 0) {
                log_message("SD_MAINT: Attempting Mount...\r\n");
                
                // Use your existing helper (retry logic inside is good)
                if (mount_sd_card_with_retry(&g_fatfs, 1, 0)) {
                    xEventGroupSetBits(xSDStateEventGroup, EVENT_SD_MOUNTED);
                    log_message("SD_MAINT: Mount Success.\r\n");
                } else {
                    log_message("SD_MAINT: Mount Failed. Retrying in 2s...\r\n");
                    vTaskDelay(pdMS_TO_TICKS(2000));
                    continue; // Loop back
                }
            }
            
            // Step B: Initialize Files (Only if Mounted)
            log_message("SD_MAINT: Opening/Creating Log Files...\r\n");
            
            // Load Metadata
            if(!load_log_metadata(&g_log_meta)) {
                // If load fails, we assume fresh start, but proceed to init file
            }
            
            // Initialize/Pre-allocate Main Log
            if (init_circular_log_file(&g_circular_log_file, &g_log_meta)) {
                 // Initialize Violation Log (Optional, logic simplified for brevity)
                 FRESULT fres_vio = f_open(&g_violation_log_file, VIOLATION_DATALOG_FILE, FA_WRITE | FA_READ | FA_OPEN_ALWAYS);
                 
                 // Mark System Ready!
                 xEventGroupSetBits(xSDStateEventGroup, EVENT_FILES_OPEN);
                 log_message("SD_MAINT: System READY for Logging.\r\n");
                 
                 // Beep to indicate success (Your original buzzer logic)
                 HAL_GPIO_WritePin(OUTPUT_BUZZER_GPIO_Port, OUTPUT_BUZZER_Pin, GPIO_PIN_SET);
                 vTaskDelay(pdMS_TO_TICKS(200));
                 HAL_GPIO_WritePin(OUTPUT_BUZZER_GPIO_Port, OUTPUT_BUZZER_Pin, GPIO_PIN_RESET);
                 
            } else {
                log_message("SD_MAINT: File Init Failed. Re-mounting...\r\n");
                // Clear mount bit to force full remount next loop
                xEventGroupClearBits(xSDStateEventGroup, EVENT_SD_MOUNTED);
                vTaskDelay(pdMS_TO_TICKS(1000));
            }
        }
        
        // Sleep while healthy. 
        // If vStorageTask fails, it clears the bits, and we catch it here eventually
        // or we can use a semaphore to wake this task up instantly on error.
        vTaskDelay(pdMS_TO_TICKS(1000));
    }
}






void vStorageTask(void *pvParameters) {
    master_log_payload_t data;
    char log_buffer[MAX_LOG_ENTRY_SIZE];
    char speed_string[12];
    UINT bytesWritten;
    
    // We assume queues and event groups are initialized
    
    for(;;) {
        // 1. Wait for Trigger AND Files Open
        // This blocks efficiently until both conditions are met.
        EventBits_t uxBits = xEventGroupWaitBits(
            xSDStateEventGroup,
            EVENT_LOG_TRIGGER | EVENT_FILES_OPEN,
            pdFALSE, // Don't clear bits automatically (we handle trigger manually)
            pdTRUE,  // Wait for ALL bits
            portMAX_DELAY
        );
        
        // 2. Clear Trigger immediately so we don't loop endlessly
        xEventGroupClearBits(xSDStateEventGroup, EVENT_LOG_TRIGGER);
        
        // 3. Process Data Queue (Drain it)
        // Using a while loop ensures we catch up if the SD card was slow previously
        while (xQueueReceive(g_sd_data_queue, &data, 0) == pdPASS) {
            
            // --- Format Data (Moved from your original loop) ---
            sprintf(speed_string, "%0.1f", data.final_log_speed);

            if (data.gps_snapshot.is_valid) {
                 snprintf(log_buffer, sizeof(log_buffer),
                         "20%02d/%02d/%02d %02d:%02d:%02d %s %.5f %.5f %s %s",
                         data.rtc_time.year, data.rtc_time.month, data.rtc_time.day_of_month,
                         data.rtc_time.hour, data.rtc_time.minutes, data.rtc_time.seconds,
                         speed_string, data.gps_snapshot.latitude, data.gps_snapshot.longitude,
                         data.power_status, data.signal_status);
            } else {
                 snprintf(log_buffer, sizeof(log_buffer),
                         "20%02d/%02d/%02d %02d:%02d:%02d %s SL SL %s %s",
                         data.rtc_time.year, data.rtc_time.month, data.rtc_time.day_of_month,
                         data.rtc_time.hour, data.rtc_time.minutes, data.rtc_time.seconds,
                         speed_string, data.power_status, data.signal_status);
            }

            // --- Padding Logic (Inline for safety) ---
            char padded_entry[MAX_LOG_ENTRY_SIZE];
            memset(padded_entry, ' ', MAX_LOG_ENTRY_SIZE);
            size_t len = strlen(log_buffer);
            if (len > MAX_LOG_ENTRY_SIZE - 2) len = MAX_LOG_ENTRY_SIZE - 2;
            memcpy(padded_entry, log_buffer, len);
            padded_entry[MAX_LOG_ENTRY_SIZE - 2] = '\r';
            padded_entry[MAX_LOG_ENTRY_SIZE - 1] = '\n';

            // --- Write to SD ---
            FRESULT res_seek = f_lseek(&g_circular_log_file, g_log_meta.current_position * MAX_LOG_ENTRY_SIZE);
            FRESULT res_write = f_write(&g_circular_log_file, padded_entry, MAX_LOG_ENTRY_SIZE, &bytesWritten);
            
            // --- Error Handling ---
            if (res_seek != FR_OK || res_write != FR_OK || bytesWritten != MAX_LOG_ENTRY_SIZE) {
                log_message("SD_STORE: Write Error! Resetting...\r\n");
                
                // CRITICAL: Clear Ready Flags. 
                // This stops this task from writing and wakes up the Maintenance Task.
                xEventGroupClearBits(xSDStateEventGroup, EVENT_FILES_OPEN);
                
                // Attempt to close safely
                f_close(&g_circular_log_file);
                
                // Break queue loop to avoid losing more data/spamming errors
                break; 
            } else {
                // Success: Update Metadata
                g_log_meta.current_position++;
                g_log_meta.total_entries_written++;
                if (g_log_meta.current_position >= TOTAL_LOG_ENTRIES) {
                    g_log_meta.current_position = 0;
                    g_log_meta.buffer_full = 1;
                }
                
                // Sync periodically (e.g., every 10 writes or just always for safety)
                f_sync(&g_circular_log_file); 
            }
        } // End Queue Loop
    }
}










