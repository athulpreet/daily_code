/* USER CODE BEGIN Header */
/**
  ******************************************************************************
  * @file           : main.c
  * @brief          : SD Card Isolated Test Program (Buffer Fixed for RAM/DMA)
  ******************************************************************************
  */
/* USER CODE END Header */

/* Includes ------------------------------------------------------------------*/
#include "main.h"
#include "fatfs.h"
#include "FreeRTOS.h"
#include "task.h"
#include "queue.h"
#include "semphr.h"
#include <string.h>
#include <stdio.h>
#include <stdarg.h>

/* ============================================================================== */
/* REAL VARIABLES (USED FOR TEST)                       */
/* ============================================================================== */
SPI_HandleTypeDef hspi2;
SPI_HandleTypeDef hspi3;
UART_HandleTypeDef huart6; // Using UART6 for Debug Console

/* SD Card Global Variables */
FATFS g_fatfs;
FIL g_fil;
FRESULT fres;

/* Task Handles */
TaskHandle_t xSDTestHandle = NULL;

/* * FIX: Define Write Buffer in RAM (Global Array) instead of const pointer.
 * ALIGN(32) ensures safety if underlying driver uses DMA/Cache.
 */
#if defined (__ICCARM__) || defined (__ARMCC_VERSION)
#pragma data_alignment=32
char g_writeBuffer[] = "Hello! This is a test string from STM32 FreeRTOS. If you see this, Write Works!";
#elif defined(__GNUC__)
__attribute__((aligned(32))) char g_writeBuffer[] = "Hello! This is a test string from STM32 FreeRTOS. If you see this, Write Works!";
#endif

/* ============================================================================== */
/* DUMMY VARIABLES (REQUIRED TO SATISFY LINKER ERRORS)              */
/* ============================================================================== */
UART_HandleTypeDef huart1;
UART_HandleTypeDef huart2;
UART_HandleTypeDef huart12;
PCD_HandleTypeDef hpcd_USB_DRD_FS;
DMA_HandleTypeDef handle_GPDMA2_Channel5;
DMA_HandleTypeDef handle_GPDMA2_Channel6;
DMA_HandleTypeDef handle_GPDMA2_Channel7;
DMA_NodeTypeDef Node_GPDMA2_Channel5;
DMA_QListTypeDef List_GPDMA2_Channel5;
DMA_NodeTypeDef Node_GPDMA2_Channel7;
DMA_QListTypeDef List_GPDMA2_Channel7;
TaskHandle_t xTaskHandler2 = NULL;

/* ============================================================================== */

/* Private function prototypes -----------------------------------------------*/
void SystemClock_Config(void);
static void MX_GPIO_Init(void);
static void MX_SPI2_Init(void);
static void MX_SPI3_Init(void);
static void MX_USART6_UART_Init(void);
void vSDCardTestTask(void *pvParameters);
void vLoggingTask(void *pvParameters);

/* FreeRTOS Hooks */
void vApplicationStackOverflowHook(TaskHandle_t xTask, char *pcTaskName) { while(1); }
void vApplicationMallocFailedHook(void) { while(1); }





//global declrations
#define MAX_LOG_MSG_LEN         256
QueueHandle_t g_log_queue;





void log_message(const char* message);





/**
  * @brief  The application entry point.
  * @retval int
  */
int main(void)
{
  HAL_Init();
  SystemClock_Config();
  MX_GPIO_Init();
  MX_SPI2_Init();
  MX_SPI3_Init();
  MX_USART6_UART_Init();
  MX_FATFS_Init();



  g_log_queue = xQueueCreate(10, MAX_LOG_MSG_LEN);

    if (g_log_queue == NULL) {

        while(1);
    }
  log_message("\r\n\r\n====================================\r\n");
  log_message("   STM32 SD Card Isolated Test   \r\n");
  log_message("====================================\r\n");

  xTaskCreate(vSDCardTestTask, "SD_Test", 2048, NULL, 5, &xSDTestHandle);
  xTaskCreate(vLoggingTask,"LoggingTask", 2024,NULL,4,NULL);




  log_message("Starting Scheduler...\r\n");
  vTaskStartScheduler();
  while (1){}
}



bool mount_sd_card_with_retry(FATFS* fatfs, uint8_t max_attempts){

FRESULT fres;
	for(uint8_t attempt=1;attempt<=max_attempts;++attempt){


		fres=f_mount(fatfs,"",1);

		if(fres==FR_OK){

			return true;
		}


		vTaskDelay(2000);
	}

	return false;
}


void log_message(const char* message){


	//if queue is not
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


//****************************TASKS**********************************************

/**
  * @brief  Task to test SD Card Read/Write functionality
  */
void vSDCardTestTask(void *pvParameters)
{


	char g_log_buffer[MAX_LOG_MSG_LEN];

    const char *testFileName = "SDTEST.TXT";
    char readBuffer[100] = {0};
    UINT bytesProcessed;
    UINT bytesToWrite = strlen(g_writeBuffer);

    // Ensure LED is OFF initially
    HAL_GPIO_WritePin(POWER_LED_GPIO_Port, POWER_LED_Pin, GPIO_PIN_RESET);

    vTaskDelay(pdMS_TO_TICKS(1000)); // Wait for hardware to settle

    log_message("[TASK] Starting SD Card Operations...\r\n");


   if( mount_sd_card_with_retry(&g_fatfs,3)==1){


	   log_message("[SUCCESS] SD CARD MOUNTED \r\n");
   }
   else{

	   log_message("[FAILED] SD CARD MOUNTED \r\n");
   }









    // 2. Open File for Writing (Create Always)
    fres = f_open(&g_fil, testFileName, FA_WRITE | FA_OPEN_ALWAYS | FA_CREATE_ALWAYS);
    if (fres != FR_OK) {



        snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "[ERROR] f_open (WRITE) failed! Error Code: %d\r\n", fres);
         log_message(g_log_buffer);


         log_message("NOTE: If Error is 19 (FR_INVALID_NAME), check filename length (8.3 format).\r\n");
        goto error_handler;
    }

    snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "[SUCCESS] File '%s' opened for writing.\r\n", testFileName);
     log_message(g_log_buffer);



    // 3. Write Data (Using RAM buffer)


    snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "Attempting write of %u bytes from RAM Address: 0x%p\r\n", bytesToWrite, g_writeBuffer);
     log_message(g_log_buffer);


    fres = f_write(&g_fil, g_writeBuffer, bytesToWrite, &bytesProcessed);

    if (fres != FR_OK || bytesProcessed != bytesToWrite) {

        snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "[ERROR] f_write failed! Error: %d, Written: %u\r\n", fres, bytesProcessed);
             log_message(g_log_buffer);

             log_message("TROUBLESHOOTING:\r\n");
             log_message("1. Check physical Lock Switch on SD Card.\r\n");
             log_message("2. Buffer is in RAM. If still failing, check Power Supply stability.\r\n");
        f_close(&g_fil);
        goto error_handler;
    }

    snprintf(g_log_buffer, MAX_LOG_MSG_LEN,"[SUCCESS] Wrote %u bytes to file.\r\n", bytesProcessed);
                log_message(g_log_buffer);

    // 4. Close File
    f_close(&g_fil);
    log_message("[SUCCESS] File closed (Data Synced).\r\n");

    vTaskDelay(pdMS_TO_TICKS(100));

    // 5. Open File for Reading
    fres = f_open(&g_fil, testFileName, FA_READ);
    if (fres != FR_OK) {

        snprintf(g_log_buffer, MAX_LOG_MSG_LEN,"[ERROR] f_open (READ) failed! Error Code: %d\r\n", fres);
                       log_message(g_log_buffer);
        goto error_handler;
    }

    snprintf(g_log_buffer, MAX_LOG_MSG_LEN,"[SUCCESS] File '%s' opened for reading.\r\n", testFileName);
                          log_message(g_log_buffer);
    // 6. Read Data
    fres = f_read(&g_fil, readBuffer, sizeof(readBuffer)-1, &bytesProcessed);
    if (fres != FR_OK) {


        snprintf(g_log_buffer, MAX_LOG_MSG_LEN,"[ERROR] f_read failed! Error Code: %d\r\n", fres);
                                  log_message(g_log_buffer);

        f_close(&g_fil);
        goto error_handler;
    }
    readBuffer[bytesProcessed] = 0; // Null terminate


    snprintf(g_log_buffer, MAX_LOG_MSG_LEN,"[SUCCESS] Read Content: \"%s\"\r\n", readBuffer);
                                     log_message(g_log_buffer);

    // 7. Close File
    f_close(&g_fil);

    // 8. Verify
    if (strcmp(g_writeBuffer, readBuffer) == 0) {
    	log_message("\r\n[RESULT] *** SD CARD TEST PASSED ***\r\n");
    } else {
    	log_message("\r\n[RESULT] *** DATA MISMATCH ***\r\n");
        goto error_handler;
    }

    // 9. Unmount
    f_mount(NULL, "", 0);

    // --- SUCCESS LOOP: Slow Blink (1s) ---
    log_message("Test Complete. LED Slow Blink = SUCCESS.\r\n");
    while(1) {
        HAL_GPIO_TogglePin(POWER_LED_GPIO_Port, POWER_LED_Pin);
        vTaskDelay(pdMS_TO_TICKS(1000));
    }

error_handler:
log_message("\r\n[RESULT] *** SD CARD TEST FAILED ***\r\n");
    // --- ERROR LOOP: Fast Blink (100ms) ---
log_message("LED Fast Blink = FAILURE.\r\n");
    while(1) {
        HAL_GPIO_TogglePin(POWER_LED_GPIO_Port, POWER_LED_Pin);
        vTaskDelay(pdMS_TO_TICKS(100));
    }
}








void vLoggingTask(void *pvParameters){

	char rx_buffer[MAX_LOG_MSG_LEN];

	log_message("-----rtos logging Task optimized \r\n");

	for(;;){



		if(xQueueReceive(g_log_queue, rx_buffer, portMAX_DELAY)==pdPASS){


			HAL_UART_Transmit(&huart6,(uint8_t*)rx_buffer,strlen(rx_buffer),100);


		}


	}



}




























/* -------------------------------------------------------------------------- */
/* Hardware Initialization Functions                                          */
/* -------------------------------------------------------------------------- */

void SystemClock_Config(void)
{
  RCC_OscInitTypeDef RCC_OscInitStruct = {0};
  RCC_ClkInitTypeDef RCC_ClkInitStruct = {0};

  /** Configure the main internal regulator output voltage */
  __HAL_PWR_VOLTAGESCALING_CONFIG(PWR_REGULATOR_VOLTAGE_SCALE2);

  while(!__HAL_PWR_GET_FLAG(PWR_FLAG_VOSRDY)) {}

  /** Initializes the RCC Oscillators */
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

  /** Initializes the CPU, AHB and APB buses clocks */
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
  hspi2.Init.BaudRatePrescaler = SPI_BAUDRATEPRESCALER_128; // Slow speed for safety
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
  hspi3.Init.BaudRatePrescaler = SPI_BAUDRATEPRESCALER_128; // Slow speed for safety
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

  /* GPIO Ports Clock Enable */
  __HAL_RCC_GPIOE_CLK_ENABLE();
  __HAL_RCC_GPIOH_CLK_ENABLE();
  __HAL_RCC_GPIOA_CLK_ENABLE();
  __HAL_RCC_GPIOC_CLK_ENABLE();
  __HAL_RCC_GPIOB_CLK_ENABLE();
  __HAL_RCC_GPIOD_CLK_ENABLE();

  /* Configure SD_CS_Pin (Important for SD Card) */
  HAL_GPIO_WritePin(GPIOB, SD_CS_Pin, GPIO_PIN_SET); // Set High (Deselect) initially

  GPIO_InitStruct.Pin = SD_CS_Pin;
  GPIO_InitStruct.Mode = GPIO_MODE_OUTPUT_PP;
  GPIO_InitStruct.Pull = GPIO_NOPULL;
  GPIO_InitStruct.Speed = GPIO_SPEED_FREQ_LOW;
  HAL_GPIO_Init(GPIOB, &GPIO_InitStruct);

  /* Configure POWER_LED_Pin for Test Feedback */
  HAL_GPIO_WritePin(POWER_LED_GPIO_Port, POWER_LED_Pin, GPIO_PIN_RESET); // Start OFF

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
