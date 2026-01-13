#include "main.h"
#include "fatfs.h"
#include "FreeRTOS.h"
#include "task.h"
#include "queue.h"
#include "semphr.h"
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdbool.h>

// --- Hardware Handles ---
SPI_HandleTypeDef hspi2;
SPI_HandleTypeDef hspi3;
UART_HandleTypeDef huart6;
UART_HandleTypeDef huart1;
UART_HandleTypeDef huart2;
UART_HandleTypeDef huart12;
PCD_HandleTypeDef hpcd_USB_DRD_FS;

// --- MISSING HANDLES RESTORED (Required by Linker) ---
// These are referenced in stm32h5xx_it.c and stm32h5xx_hal_msp.c
DMA_HandleTypeDef handle_GPDMA2_Channel5;
DMA_NodeTypeDef Node_GPDMA2_Channel5;
DMA_QListTypeDef List_GPDMA2_Channel5;

DMA_HandleTypeDef handle_GPDMA2_Channel6;

DMA_HandleTypeDef handle_GPDMA2_Channel7;
DMA_NodeTypeDef Node_GPDMA2_Channel7;
DMA_QListTypeDef List_GPDMA2_Channel7;

TaskHandle_t xTaskHandler2 = NULL;
// -----------------------------------------------------

// --- FatFs Objects ---
FATFS g_fatfs;
FIL g_fil;
FRESULT fres;

// --- RTOS Objects ---
TaskHandle_t xSDTestHandle = NULL;
QueueHandle_t g_log_queue;

// --- Logging Definitions ---
#define MAX_LOG_MSG_LEN         256
char g_log_buffer[MAX_LOG_MSG_LEN]; // Global buffer for snprintf

// Use the global instance from your headers if it exists, or define locally if needed
LogMetadata g_log_meta;

// --- Function Prototypes ---
void SystemClock_Config(void);
static void MX_GPIO_Init(void);
static void MX_SPI2_Init(void);
static void MX_SPI3_Init(void);
static void MX_USART6_UART_Init(void);
// Note: MX_GPDMA2_Init removed to prevent conflicts if we aren't initializing it fully,
// but the handles above must exist.

void vSDCardTestTask(void *pvParameters);
void vLoggingTask(void *pvParameters);
void log_message(const char* message);
bool init_circular_log_file(FIL *logFile, LogMetadata *metadata);

// --- Hooks ---
void vApplicationStackOverflowHook(TaskHandle_t xTask, char *pcTaskName) { while(1); }
void vApplicationMallocFailedHook(void) { while(1); }

// --- Main ---
int main(void)
{
  HAL_Init();
  SystemClock_Config();
  MX_GPIO_Init();
  // We initialize the specific peripherals needed for SD card
  MX_SPI2_Init();
  MX_SPI3_Init();
  MX_USART6_UART_Init();
  MX_FATFS_Init();

  // Create Log Queue
  g_log_queue = xQueueCreate(10, MAX_LOG_MSG_LEN);
  if (g_log_queue == NULL) { while(1); }

  log_message("\r\n\r\n====================================\r\n");
  log_message("   STM32 Circular Log Test (Isolated)   \r\n");
  log_message("====================================\r\n");

  // Create Tasks
  xTaskCreate(vSDCardTestTask, "SD_Test", 4096, NULL, 5, &xSDTestHandle);
  xTaskCreate(vLoggingTask, "LoggingTask", 2048, NULL, 4, NULL);

  log_message("Starting Scheduler...\r\n");
  vTaskStartScheduler();
  while (1){}
}

// --- Helper Functions ---

void log_message(const char* message){
    if(g_log_queue == NULL) return;
    BaseType_t xHigherPriorityTaskWoken = pdFALSE;

    if(xPortIsInsideInterrupt()){
        xQueueSendFromISR(g_log_queue, message, &xHigherPriorityTaskWoken);
        portYIELD_FROM_ISR(xHigherPriorityTaskWoken);
    }
    else{
        xQueueSend(g_log_queue,message,pdMS_TO_TICKS(10));
    }
}

// --- TASKS ---

void vLoggingTask(void *pvParameters){
    char rx_buffer[MAX_LOG_MSG_LEN];
    for(;;){
        if(xQueueReceive(g_log_queue, rx_buffer, portMAX_DELAY)==pdPASS){
            HAL_UART_Transmit(&huart6,(uint8_t*)rx_buffer,strlen(rx_buffer),100);
        }
    }
}

/**
 * @brief  Initialize circular log file by writing 4KB chunks
 *         NO f_lseek - writes actual blank entry data
 */
bool init_circular_log_file(FIL *logFile, LogMetadata *metadata) {
    FRESULT fres;
    FSIZE_t expected_size = (FSIZE_t)TOTAL_LOG_ENTRIES * MAX_LOG_ENTRY_SIZE;

    log_message("LOG_INIT: Creating new file. Waiting 1s for stability...\r\n");
    vTaskDelay(pdMS_TO_TICKS(1000));

    snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "LOG_INIT: Target size: %lu bytes\r\n", (unsigned long)expected_size);
    log_message(g_log_buffer);

    // --- Use 4KB chunks instead of 512B (8x fewer writes) ---
    const uint32_t BUFFER_SIZE = 4096;
    const uint32_t ENTRIES_PER_BUFFER = BUFFER_SIZE / MAX_LOG_ENTRY_SIZE;

    uint8_t *large_buffer = pvPortMalloc(BUFFER_SIZE);
    if (large_buffer == NULL) {
        log_message("LOG_INIT: Memory allocation failed!\r\n");
        return false;
    }

    // Fill entire 4KB buffer with blank entries
    for(uint32_t k=0; k < ENTRIES_PER_BUFFER; k++) {
        uint32_t offset = k * MAX_LOG_ENTRY_SIZE;
        memset(&large_buffer[offset], ' ', MAX_LOG_ENTRY_SIZE);
        large_buffer[offset + MAX_LOG_ENTRY_SIZE - 2] = '\r';
        large_buffer[offset + MAX_LOG_ENTRY_SIZE - 1] = '\n';
    }

    // Create file
    fres = f_open(logFile, DATALOG_FILE, FA_WRITE | FA_CREATE_ALWAYS);
    if (fres != FR_OK) {
        snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "LOG_INIT: Create FAILED! Error=%d\r\n", fres);
        log_message(g_log_buffer);
        vPortFree(large_buffer);
        return false;
    }

    log_message("LOG_INIT: Writing 4KB chunks...\r\n");

    uint32_t total_writes = TOTAL_LOG_ENTRIES / ENTRIES_PER_BUFFER;

    snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "LOG_INIT: Total 4KB chunks: %lu\r\n", (unsigned long)total_writes);
    log_message(g_log_buffer);

    // Write loop
    for (uint32_t i = 0; i < total_writes; i++) {
        UINT bw;
        int retries = 5;

        while (retries > 0) {
            fres = f_write(logFile, large_buffer, BUFFER_SIZE, &bw);

            if (fres == FR_OK && bw == BUFFER_SIZE) {
                break; // Success
            }

            retries--;
            if (retries == 0) {
                 snprintf(g_log_buffer, MAX_LOG_MSG_LEN,
                          "LOG_INIT: FATAL at chunk %lu\r\n  fres=%d, bw=%u, FilePos=%lu, FileSize=%lu\r\n",
                          (unsigned long)i, fres, bw,
                          (unsigned long)f_tell(logFile), (unsigned long)f_size(logFile));
                 log_message(g_log_buffer);
                 f_close(logFile);
                 f_unlink(DATALOG_FILE);
                 vPortFree(large_buffer);
                 return false;
            }

            snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "LOG_INIT: Retry %d at chunk %lu...\r\n",
                     (6-retries), (unsigned long)i);
            log_message(g_log_buffer);
            vTaskDelay(pdMS_TO_TICKS(500)); // Wait 500ms between retries
        }

        // Progress every 50 chunks (~200KB)
        if (i % 50 == 0 && i > 0) {
            snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "LOG_INIT: Progress: %lu/%lu (Size=%lu)\r\n",
                     (unsigned long)i, (unsigned long)total_writes, (unsigned long)f_size(logFile));
            log_message(g_log_buffer);
        }

        // Small delay every 10 chunks (~40KB)
        if (i % 10 == 0) {
            vTaskDelay(pdMS_TO_TICKS(20));
        }

        // Sync only every 100 chunks (~400KB)
        if (i > 0 && i % 100 == 0) {
            f_sync(logFile);
            vTaskDelay(pdMS_TO_TICKS(500));
        }
    }

    vPortFree(large_buffer);

    // Final sync
    f_sync(logFile);
    vTaskDelay(pdMS_TO_TICKS(1000));

    log_message("LOG_INIT: Allocation complete. Verifying...\r\n");

    FSIZE_t actual_size = f_size(logFile);
    snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "LOG_INIT: Expected:%lu Actual:%lu\r\n",
             (unsigned long)expected_size, (unsigned long)actual_size);
    log_message(g_log_buffer);

    if (actual_size < expected_size) {
        log_message("LOG_INIT: Size mismatch warning!\r\n");
    }

    // Reset Metadata
    metadata->current_position = 0;
    metadata->total_entries_written = 0;
    metadata->buffer_full = 0;

    log_message("LOG_INIT: Circular log ready.\r\n");
    return true;
}

/**
  * @brief  Task to test SD Card Read/Write functionality
  */
void vSDCardTestTask(void *pvParameters)
{
    // Ensure LED is OFF initially
    HAL_GPIO_WritePin(POWER_LED_GPIO_Port, POWER_LED_Pin, GPIO_PIN_RESET);
    vTaskDelay(pdMS_TO_TICKS(1000)); // Wait for hardware to settle

    log_message("[TASK] Mounting SD Card...\r\n");

    // 1. Mount
    FRESULT res = f_mount(&g_fatfs, "", 1);
    if (res != FR_OK) {
        snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "[ERROR] Mount failed! Error: %d\r\n", res);
        log_message(g_log_buffer);
        goto error_handler;
    }
    log_message("[SUCCESS] SD Card Mounted.\r\n");

    // 2. Initialize Circular Log
    if (!init_circular_log_file(&g_fil, &g_log_meta)) {
        log_message("[ERROR] Circular Log Init Failed!\r\n");
        goto error_handler;
    }

    // 3. Test Writing to Circular Log (Simulate Usage)
    log_message("[TEST] Writing 5 test entries...\r\n");

    char testEntry[MAX_LOG_ENTRY_SIZE];
    UINT bw;

    // Close the handle from Init and Re-open for Read/Write
    f_close(&g_fil);
    f_open(&g_fil, DATALOG_FILE, FA_READ | FA_WRITE);

    for(int i=0; i<5; i++) {
        // Prepare a test entry
        memset(testEntry, ' ', MAX_LOG_ENTRY_SIZE);
        snprintf(testEntry, MAX_LOG_ENTRY_SIZE, "Log Entry Number %d -- TEST -- ", i);
        // Ensure CRLF at end of fixed size
        testEntry[MAX_LOG_ENTRY_SIZE - 2] = '\r';
        testEntry[MAX_LOG_ENTRY_SIZE - 1] = '\n';

        // Seek to current position (simple 0-4 for this test)
        f_lseek(&g_fil, i * MAX_LOG_ENTRY_SIZE);

        res = f_write(&g_fil, testEntry, MAX_LOG_ENTRY_SIZE, &bw);

        if (res == FR_OK && bw == MAX_LOG_ENTRY_SIZE) {
             snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "  > Wrote entry %d OK\r\n", i);
             log_message(g_log_buffer);
        } else {
             log_message("  > Write Failed!\r\n");
        }
        f_sync(&g_fil);
        vTaskDelay(pdMS_TO_TICKS(50));
    }

    f_close(&g_fil);
    log_message("[SUCCESS] Test Complete. \r\n");

    while(1) {
        HAL_GPIO_TogglePin(POWER_LED_GPIO_Port, POWER_LED_Pin);
        vTaskDelay(pdMS_TO_TICKS(1000));
    }

error_handler:
    log_message("[FAIL] Critical Error. \r\n");
    while(1) {
        HAL_GPIO_TogglePin(POWER_LED_GPIO_Port, POWER_LED_Pin);
        vTaskDelay(pdMS_TO_TICKS(100));
    }
}

// ... [Keep your Hardware Init Functions here: SystemClock_Config, etc.] ...

void SystemClock_Config(void)
{
  RCC_OscInitTypeDef RCC_OscInitStruct = {0};
  RCC_ClkInitTypeDef RCC_ClkInitStruct = {0};

  __HAL_PWR_VOLTAGESCALING_CONFIG(PWR_REGULATOR_VOLTAGE_SCALE2);

  while(!__HAL_PWR_GET_FLAG(PWR_FLAG_VOSRDY)) {}

  RCC_OscInitStruct.OscillatorType = RCC_OSCILLATORTYPE_HSI48|RCC_OSCILLATORTYPE_HSE;
  RCC_OscInitStruct.HSEState = RCC_HSE_ON;
  RCC_OscInitStruct.HSI48State = RCC_HSI48_ON;
  RCC_OscInitStruct.PLL.PLLState = RCC_PLL_ON;
  RCC_OscInitStruct.PLL.PLLSource = RCC_PLL1_SOURCE_HSE;
  RCC_OscInitStruct.PLL.PLLM = 1;
  RCC_OscInitStruct.PLL.PLLN = 36;
  RCC_OscInitStruct.PLL.PLLP = 2;
  RCC_OscInitStruct.PLL.PLLQ = 10;
  RCC_OscInitStruct.PLL.PLLR = 2;
  RCC_OscInitStruct.PLL.PLLRGE = RCC_PLL1_VCIRANGE_3;
  RCC_OscInitStruct.PLL.PLLVCOSEL = RCC_PLL1_VCORANGE_WIDE;
  RCC_OscInitStruct.PLL.PLLFRACN = 6144;
  if (HAL_RCC_OscConfig(&RCC_OscInitStruct) != HAL_OK)
  {
    while(1);
  }

  RCC_ClkInitStruct.ClockType = RCC_CLOCKTYPE_HCLK|RCC_CLOCKTYPE_SYSCLK
                              |RCC_CLOCKTYPE_PCLK1|RCC_CLOCKTYPE_PCLK2
                              |RCC_CLOCKTYPE_PCLK3;
  RCC_ClkInitStruct.SYSCLKSource = RCC_SYSCLKSOURCE_PLLCLK;
  RCC_ClkInitStruct.AHBCLKDivider = RCC_SYSCLK_DIV1;
  RCC_ClkInitStruct.APB1CLKDivider = RCC_HCLK_DIV1;
  RCC_ClkInitStruct.APB2CLKDivider = RCC_HCLK_DIV1;
  RCC_ClkInitStruct.APB3CLKDivider = RCC_HCLK_DIV1;

  if (HAL_RCC_ClockConfig(&RCC_ClkInitStruct, FLASH_LATENCY_4) != HAL_OK)
  {
    while(1);
  }

  HAL_RCC_EnableCSS();
}

static void MX_SPI2_Init(void)
{
  hspi2.Instance = SPI2;
  hspi2.Init.Mode = SPI_MODE_MASTER;
  hspi2.Init.Direction = SPI_DIRECTION_2LINES;
  hspi2.Init.DataSize = SPI_DATASIZE_8BIT;
  hspi2.Init.CLKPolarity = SPI_POLARITY_LOW;
  hspi2.Init.CLKPhase = SPI_PHASE_1EDGE;
  hspi2.Init.NSS = SPI_NSS_SOFT;
  // NOTE: Ensure your user_diskio_spi.c handles the speed switch!
  // This init speed is for the bus, but driver usually overrides.
  hspi2.Init.BaudRatePrescaler = SPI_BAUDRATEPRESCALER_128;
  hspi2.Init.FirstBit = SPI_FIRSTBIT_MSB;
  hspi2.Init.TIMode = SPI_TIMODE_DISABLE;
  hspi2.Init.CRCCalculation = SPI_CRCCALCULATION_DISABLE;
  hspi2.Init.CRCPolynomial = 0x7;
  hspi2.Init.NSSPMode = SPI_NSS_PULSE_DISABLE;
  hspi2.Init.NSSPolarity = SPI_NSS_POLARITY_LOW;
  hspi2.Init.FifoThreshold = SPI_FIFO_THRESHOLD_01DATA;
  hspi2.Init.MasterSSIdleness = SPI_MASTER_SS_IDLENESS_00CYCLE;
  hspi2.Init.MasterInterDataIdleness = SPI_MASTER_INTERDATA_IDLENESS_00CYCLE;
  hspi2.Init.MasterReceiverAutoSusp = SPI_MASTER_RX_AUTOSUSP_DISABLE;
  hspi2.Init.MasterKeepIOState = SPI_MASTER_KEEP_IO_STATE_DISABLE;
  hspi2.Init.IOSwap = SPI_IO_SWAP_DISABLE;
  hspi2.Init.ReadyMasterManagement = SPI_RDY_MASTER_MANAGEMENT_INTERNALLY;
  hspi2.Init.ReadyPolarity = SPI_RDY_POLARITY_HIGH;
  if (HAL_SPI_Init(&hspi2) != HAL_OK)
  {
    while(1);
  }
}

static void MX_SPI3_Init(void)
{
  hspi3.Instance = SPI3;
  hspi3.Init.Mode = SPI_MODE_MASTER;
  hspi3.Init.Direction = SPI_DIRECTION_2LINES;
  hspi3.Init.DataSize = SPI_DATASIZE_8BIT;
  hspi3.Init.CLKPolarity = SPI_POLARITY_LOW;
  hspi3.Init.CLKPhase = SPI_PHASE_1EDGE;
  hspi3.Init.NSS = SPI_NSS_SOFT;
  hspi3.Init.BaudRatePrescaler = SPI_BAUDRATEPRESCALER_128;
  hspi3.Init.FirstBit = SPI_FIRSTBIT_MSB;
  hspi3.Init.TIMode = SPI_TIMODE_DISABLE;
  hspi3.Init.CRCCalculation = SPI_CRCCALCULATION_DISABLE;
  hspi3.Init.CRCPolynomial = 0x7;
  hspi3.Init.NSSPMode = SPI_NSS_PULSE_ENABLE;
  hspi3.Init.NSSPolarity = SPI_NSS_POLARITY_LOW;
  hspi3.Init.FifoThreshold = SPI_FIFO_THRESHOLD_01DATA;
  hspi3.Init.MasterSSIdleness = SPI_MASTER_SS_IDLENESS_00CYCLE;
  hspi3.Init.MasterInterDataIdleness = SPI_MASTER_INTERDATA_IDLENESS_00CYCLE;
  hspi3.Init.MasterReceiverAutoSusp = SPI_MASTER_RX_AUTOSUSP_DISABLE;
  hspi3.Init.MasterKeepIOState = SPI_MASTER_KEEP_IO_STATE_DISABLE;
  hspi3.Init.IOSwap = SPI_IO_SWAP_DISABLE;
  hspi3.Init.ReadyMasterManagement = SPI_RDY_MASTER_MANAGEMENT_INTERNALLY;
  hspi3.Init.ReadyPolarity = SPI_RDY_POLARITY_HIGH;
  if (HAL_SPI_Init(&hspi3) != HAL_OK)
  {
    while(1);
  }
}

static void MX_USART6_UART_Init(void)
{
  huart6.Instance = USART6;
  huart6.Init.BaudRate = 115200;
  huart6.Init.WordLength = UART_WORDLENGTH_8B;
  huart6.Init.StopBits = UART_STOPBITS_1;
  huart6.Init.Parity = UART_PARITY_NONE;
  huart6.Init.Mode = UART_MODE_TX_RX;
  huart6.Init.HwFlowCtl = UART_HWCONTROL_NONE;
  huart6.Init.OverSampling = UART_OVERSAMPLING_16;
  huart6.Init.OneBitSampling = UART_ONE_BIT_SAMPLE_DISABLE;
  huart6.Init.ClockPrescaler = UART_PRESCALER_DIV1;
  huart6.AdvancedInit.AdvFeatureInit = UART_ADVFEATURE_NO_INIT;
  if (HAL_UART_Init(&huart6) != HAL_OK)
  {
    while(1);
  }
}

static void MX_GPIO_Init(void)
{
  GPIO_InitTypeDef GPIO_InitStruct = {0};

  __HAL_RCC_GPIOE_CLK_ENABLE();
  __HAL_RCC_GPIOH_CLK_ENABLE();
  __HAL_RCC_GPIOA_CLK_ENABLE();
  __HAL_RCC_GPIOC_CLK_ENABLE();
  __HAL_RCC_GPIOB_CLK_ENABLE();
  __HAL_RCC_GPIOD_CLK_ENABLE();

  /* Configure SD_CS_Pin */
  HAL_GPIO_WritePin(GPIOB, SD_CS_Pin, GPIO_PIN_SET);

  GPIO_InitStruct.Pin = SD_CS_Pin;
  GPIO_InitStruct.Mode = GPIO_MODE_OUTPUT_PP;
  GPIO_InitStruct.Pull = GPIO_NOPULL;
  GPIO_InitStruct.Speed = GPIO_SPEED_FREQ_LOW;
  HAL_GPIO_Init(GPIOB, &GPIO_InitStruct);

  /* Configure POWER_LED_Pin */
  HAL_GPIO_WritePin(POWER_LED_GPIO_Port, POWER_LED_Pin, GPIO_PIN_RESET);

  GPIO_InitStruct.Pin = POWER_LED_Pin;
  GPIO_InitStruct.Mode = GPIO_MODE_OUTPUT_PP;
  GPIO_InitStruct.Pull = GPIO_NOPULL;
  GPIO_InitStruct.Speed = GPIO_SPEED_FREQ_LOW;
  HAL_GPIO_Init(POWER_LED_GPIO_Port, &GPIO_InitStruct);
}

void HAL_TIM_PeriodElapsedCallback(TIM_HandleTypeDef *htim)
{
  if (htim->Instance == TIM5) {
    HAL_IncTick();
  }
}

void Error_Handler(void)
{
  __disable_irq();
  while (1) {}
}
