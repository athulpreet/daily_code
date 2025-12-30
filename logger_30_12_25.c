







#include "main.h"
#include "FreeRTOS.h"
#include "queue.h"
#include "semphr.h"
#include "task.h"
#include "fatfs.h"
#include <string.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <time.h>
#include <math.h>
#include <stdarg.h>
#include <stdlib.h>
#include <ctype.h>
#include <device_log_processing.h>
#include "usbd_core.h"
#include "usbd_cdc.h"
#include "usbd_cdc_if.h"
#include "usbd_hid.h"
#include "usbd_desc.h"
#include "usbd_composite_builder.h"
#include <stdio.h>
#include "global_declarations.h"
#include "handheld_programing.h"

typedef struct {
    float latitude;
    float longitude;
    float altitude;
    float speed_knots;
    float course_degrees;
    char time[12];
    char date[8];
    bool is_valid;
} gps_data_t;

float gps_speed_kmhr=0.0;


typedef struct {
    uint8_t seconds;
    uint8_t minutes;
    uint8_t hour;
    uint8_t day_of_week;
    uint8_t day_of_month;
    uint8_t month;
    uint8_t year;
} RTC_Time;

extern LogMetadata g_log_meta;



typedef struct {
    RTC_Time rtc_time;          // The single, master timestamp from DS3231
    gps_data_t gps_snapshot;    // The GPS data (lat, lon, speed_knots, valid)

    float final_log_speed;      // This will be your 'actual_vehicle_spped'
    float msld_speed;           // This will be your 'speed_rng' (for the msldSpeed_kmh field)
    char power_status[6];       // "PowC" / "PowD"
    char signal_status[6];      // "SigC" / "SigD"
    char vehicle_status[10];    // "IDLE" / "RUNNING"
} master_log_payload_t;


char g_device_id[32] = "THIN0011";




uint8_t CDC_EpAdd_Inst[3] = {CDC_IN_EP, CDC_OUT_EP, CDC_CMD_EP}; 	/* CDC Endpoint Addresses array */
uint8_t HID_EpAdd_Inst = HID_EPIN_ADDR;								/* HID Endpoint Address array */
USBD_HandleTypeDef hUsbDeviceFS;
uint8_t hid_report_buffer[4];
uint8_t HID_InstID = 0, CDC_InstID = 0;

#define MAX_LOG_MSG_LEN         256
#define LOG_QUEUE_SIZE          20


#define USE_SECONDARY_SERVER            1       // Set to 1 to enable, 0 to disable
#define MODEM_UART_RX_BUFFER_SIZE       256
#define MODEM_PWR_RST_GPIO_Port         GPIOB
#define MODEM_PWR_RST_Pin               GPIO_PIN_9
#define IDLE_SPEED_THRESHOLD_KNOTS      2.0f
#define MODEM_UART_RX_QUEUE_LEN         5


#define DS3231_I2C_ADDR  (0x68 << 1)
#define DS3231_REG_SECONDS   0x00

int his_sequence_number=0;
char *header_details =
		"THINTURE TECHNOLOGIES PVT LTD\n"
		"OWNER'S NAME\n"
		"OWNERS ID\n"
		"+9199959XXXXX\n"
		"KA 01 78XX\n"
		"CH123456789\n"
		"VEHICLE MAKE\n"
		"SPEED LIMITER CERT NO:\n"
		"SPEED LIMITER TYPE\n"
		"SER NO:\n"
		"DATE OF FITTING\n"
		"\n";




#define AUTH_SENDER_EEPROM_ADDR     300
#define AUTH_SENDER_MAX_LEN         20
#define DEFAULT_AUTH_SENDER         "+919110470625" // Fallback if EEPROM is empty

#define MQTT_BROKER_IP_ADDR         320 // Size: 40
#define MQTT_BROKER_PORT_ADDR       360 // Size: 6
#define MQTT_CLIENT_ID_ADDR         366 // Size: 32
#define MQTT_USERNAME_ADDR          398 // Size: 32
#define MQTT_PASSWORD_ADDR          430 // Size: 32
#define MQTT_TOPIC_ADDR             462 // Size: 32

#define SEC_MQTT_BROKER_IP_ADDR     500 // Size: 40
#define SEC_MQTT_BROKER_PORT_ADDR   540 // Size: 6
#define SEC_MQTT_CLIENT_ID_ADDR     546 // Size: 32
#define SEC_MQTT_USERNAME_ADDR      578 // Size: 32
#define SEC_MQTT_PASSWORD_ADDR      610 // Size: 32
#define SEC_MQTT_TOPIC_ADDR         642 // Size: 32

#define DEVICE_ID_ADDR 674 //

#define TIMEZONE_EEPROM_ADDR    	706
#define TIMEZONE_MAX_LEN        	8   // For "+05:30\0"
char g_time_zone_str[TIMEZONE_MAX_LEN] = "+03:00";
volatile bool g_update_rtc_from_gps_flag = false;
volatile bool g_initial_rtc_sync_done = false;
bool sync_rtc_with_gps(const char* utc_date_str, const char* utc_time_str, const char* tz_str);

void rtc_to_utc_timestamp(RTC_Time timeDate);


ADC_HandleTypeDef hadc1;
ADC_HandleTypeDef hadc2;

I2C_HandleTypeDef hi2c1;
I2C_HandleTypeDef hi2c4;

SPI_HandleTypeDef hspi2;
SPI_HandleTypeDef hspi3;

TIM_HandleTypeDef htim2;

UART_HandleTypeDef huart12;
UART_HandleTypeDef huart1;
UART_HandleTypeDef huart2;
UART_HandleTypeDef huart6;
DMA_NodeTypeDef Node_GPDMA2_Channel7;
DMA_QListTypeDef List_GPDMA2_Channel7;
DMA_HandleTypeDef handle_GPDMA2_Channel7;
DMA_HandleTypeDef handle_GPDMA2_Channel6;
DMA_NodeTypeDef Node_GPDMA2_Channel5;
DMA_QListTypeDef List_GPDMA2_Channel5;
DMA_HandleTypeDef handle_GPDMA2_Channel5;

PCD_HandleTypeDef hpcd_USB_DRD_FS;



QueueHandle_t handheld_uart_queue;
TaskHandle_t xTaskHandler = NULL;
TaskHandle_t xTaskHandler1 = NULL;
TaskHandle_t xTaskHandler2 = NULL;
TaskHandle_t xTaskHandler3= NULL;
TaskHandle_t xTaskHandler4 = NULL;
TaskHandle_t xTaskHandler5 = NULL;

SemaphoreHandle_t handheld_data_ready_sem;
uint8_t uart2_rx_size=0;

int gps_eeprom_status=0;
int buzz_off_flag=0;
int pedal_exit_flag=0;


struct timer_struct
{
	int8_t st_CFlg;
	int16_t st_counter;
	int8_t st_SFlg;
};
struct timer_struct serial_speed_receive;
struct timer_struct stack_size_tester;
struct timer_struct control_buzz_onoff;
struct timer_struct pid_pedal_exit;
struct timer_struct signal_disconnection;
struct timer_struct dma_hand_held;
struct timer_struct limb_control;
struct timer_struct speed_signal_limb;

uint16_t timer_counter_ivms=50;
float output_pedal_value=0.0;
float pid_result=0.0;
uint8_t rx_high_set_speed_flag=0;
uint16_t dma_delay_serial=5;
uint8_t serial_data_true=0;
float pid_steady_val=0.0;
float previous_speed=0.0;
uint16_t PRINT_DEBUG_TIME=0;

volatile float ppr_value=0;
float spd=0;
int spd_exit=0;

float error_value=0;
float I_=0;
float I_Clamping=0;
float pre_error_value=0;

uint8_t pid_loop_condition=0;

int8_t cf,buzz;
int8_t set_speed_flag=0;

unsigned char ser_speed;
int8_t spd_change=0;

volatile float speed_rng=0;
float error_percentage=0.0;
float steady_value=0;
uint8_t ch_ok;
float pid_return=0;
uint8_t speed_exit_flag;
uint8_t set_speed_rx_flg=0;


float speed=0.0;
uint8_t can_speedx=0;

uint16_t counter_send_return=0;
int rx_uart3_flag=0;
int ivms_uart3_flag=0;

int Is_First_Captured=0;
float frequency = 0;

int frequency_flag=0;
uint8_t ch_ok;
uint32_t Timer_count=0;
uint8_t power_input=0;
uint8_t ignition_input=0;

int get_gps_fast=0;

int global_msld_speed=0;
int vehicleIgnitionStatus=0;
char ignitionStatus[9];

int device_set_speed=0;
int device_limp_speed=0;
char signal_status[6];
char power_status[6];
int power_input_status=0;

char header_data_array[384];
#define APP_RX_DATA_SIZE  758
extern uint8_t UserRxBufferFS[APP_RX_DATA_SIZE];

int his_vehicle_status=0;
int his_ign_status=0;
int his_power_cut=0;
int his_signal_cut=0;



SemaphoreHandle_t g_gps_data_ready_sem;
SemaphoreHandle_t g_gps_data_mutex;
SemaphoreHandle_t g_sd_card_mutex;
gps_data_t g_gps_data = {0};


#define GPS_DMA_RX_BUFFER_SIZE      512
#define MAX_NMEA_SENTENCE_LEN       100
#define MAX_SPEED_METERS_PER_SECOND 85.0
uint8_t g_gps_dma_rx_buffer[GPS_DMA_RX_BUFFER_SIZE];


static uint16_t read_pos = 0;
volatile uint16_t write_pos = 0;
static gps_data_t g_last_valid_gps_data = { .is_valid = false };


QueueHandle_t g_log_queue;
char g_log_buffer[MAX_LOG_MSG_LEN];


QueueHandle_t g_modem_uart_rx_queue;
QueueHandle_t g_data_snapshot_queue;
uint8_t g_modem_dma_rx_buffer[MODEM_UART_RX_BUFFER_SIZE];
char g_apn[32] = "internet";


char g_mqtt_broker_ip[40] 	= "3.109.116.92";
char g_mqtt_broker_port[6] 	= "1883";
char g_mqtt_client_id[32] 	= "spring-client";
char g_mqtt_username[32] 	= "Thinture";
char g_mqtt_password[32]	= "Thinture24";
char g_mqtt_topic[32] 		= "Test";


#if USE_SECONDARY_SERVER
char g_secondary_mqtt_broker_ip[40] 	= "43.205.58.131";
char g_secondary_mqtt_broker_port[6] 	= "1884";
char g_secondary_mqtt_client_id[32] 	= "spring-client-secondary";
char g_secondary_mqtt_username[32] 		= "Thinture";
char g_secondary_mqtt_password[32] 		= "Thinture";
char g_secondary_mqtt_topic[32] 		= "Test";
#endif


FATFS g_fatfs;
FIL g_circular_log_file;
LogMetadata g_log_meta;


FIL g_violation_log_file;
LogMetadata g_violation_log_meta;
volatile bool g_log_violation_event = false;


volatile bool g_gps_fix_acquired = false;
volatile bool g_gsm_connection_acquired = false;


SemaphoreHandle_t g_modem_mutex;
volatile uint32_t g_successful_publishes = 0;
volatile uint32_t g_session_start_time = 0;


char g_authorized_sender[AUTH_SENDER_MAX_LEN];


void SystemClock_Config(void);
static void MX_GPIO_Init(void);
static void MX_GPDMA2_Init(void);
static void MX_ADC2_Init(void);
static void MX_I2C1_Init(void);
static void MX_I2C4_Init(void);
static void MX_SPI2_Init(void);
static void MX_SPI3_Init(void);
static void MX_TIM2_Init(void);
static void MX_UART12_Init(void);
static void MX_USART1_UART_Init(void);
static void MX_USART2_UART_Init(void);
static void MX_USART6_UART_Init(void);
static void MX_ICACHE_Init(void);
static void MX_ADC1_Init(void);




void vStatusLedTask(void *pvParameters);
void vGpsTask(void *pvParameters);
void vLoggingTask(void *pvParameters);
void vMqttTask(void *pvParameters);
void vCircularTask(void *pvParameters);

void convert_sequence(uint16_t n, char *out,uint16_t block_size_val);
void format_timestamp_fixed_offset(time_t ts, int offset_seconds, char *buf, size_t bufsize);
void rtc_to_utc_timestamp(RTC_Time timeDate);
void packetize_history_data();
uint8_t publish_history_data(char *history_data_payload_);

bool format_sdcard(void);


void vSmsTask(void *pvParameters);
bool send_sms(const char* recipient, const char* message);
void flush_modem_uart_buffer(void);


void eeprom_write_string(uint16_t address, const char* str, uint16_t max_len);
bool eeprom_read_string(uint16_t address, char* buffer, uint16_t max_len);
void load_persistent_settings(void);


void dev_set( void *pvParameters );
void timer_jobs(void *pvParameters);
void vHandTask( void *pvParameters );
void iHandTask( void *pvParameters );
void SYHandTask( void *pvParameters );
void time_jobs( void *pvParameters );
void HH_dma( void *pvParameters );
void process_uart2_buffer(uint8_t* buffer, uint16_t size);



bool send_and_wait_for_response(const char* cmd, const char* expected_response, uint32_t timeout_ms);
void perform_modem_power_cycle(void);
void format_coord(char* buffer, size_t buffer_size, float coord, char positive_dir, char negative_dir);
void convert_utc_datetime_to_ist(const char* utc_date_str, const char* utc_time_str, char* ist_buffer, size_t buffer_size);


void DS3231_SetTime(RTC_Time *time);
void DS3231_GetTime(RTC_Time *time);


bool mount_sd_card_with_retry(FATFS* fatfs, uint8_t max_attempts, uint32_t retry_delay_ms);
bool load_log_metadata(LogMetadata *metadata);
bool save_log_metadata(LogMetadata *metadata);
bool init_circular_log_file(FIL *logFile, LogMetadata *metadata);
bool write_circular_log_entry(FIL *logFile, LogMetadata *metadata, const char *log_entry);



void vApplicationStackOverflowHook(TaskHandle_t xTask, char *pcTaskName)
{
    taskENTER_CRITICAL();

}

void vApplicationMallocFailedHook(void)
{
    taskENTER_CRITICAL();

}

void log_message(const char* message) {
    if (g_log_queue == NULL) return;

    BaseType_t xHigherPriorityTaskWoken = pdFALSE;


    if (xPortIsInsideInterrupt()) {

        xQueueSendFromISR(g_log_queue, message, &xHigherPriorityTaskWoken);
        portYIELD_FROM_ISR(xHigherPriorityTaskWoken);
    }
    else {

        xQueueSend(g_log_queue, message, pdMS_TO_TICKS(10));
    }
}


void eeprom_write_string(uint16_t address, const char* str, uint16_t max_len) {
    uint16_t i;
    for (i = 0; i < max_len - 1; i++) {

        eeprom_WRITEBYTE(address + i, str[i]);
        vTaskDelay(pdMS_TO_TICKS(5));
        if (str[i] == '\0') {
            break;
        }
    }

    eeprom_WRITEBYTE(address + i, '\0');
    vTaskDelay(pdMS_TO_TICKS(5));
}


bool eeprom_read_string(uint16_t address, char* buffer, uint16_t max_len) {
    bool is_valid = false;
    for (uint16_t i = 0; i < max_len; i++) {

        buffer[i] = eeprom_READBYTE(address + i);
        if (i == 0 && (buffer[i] == 0xFF || buffer[i] == 0x00)) {

            is_valid = false;
            break;
        }
        if (buffer[i] == '\0') {
            is_valid = true;
            break;
        }
        if (buffer[i] == 0xFF) {

            buffer[i] = '\0';
            is_valid = true;
            break;
        }
    }
    buffer[max_len - 1] = '\0';
    return is_valid;
}


void load_persistent_settings(void) {
    log_message("Loading All Persistent-\r\n");
    bool needs_saving = false;


    if (!eeprom_read_string(AUTH_SENDER_EEPROM_ADDR, g_authorized_sender, AUTH_SENDER_MAX_LEN) ||
        (strcmp(g_authorized_sender, "UNAUTHORIZED") != 0 && g_authorized_sender[0] != '+'))
    {
        log_message("AUTH: No valid sender.\r\n");
        strcpy(g_authorized_sender, DEFAULT_AUTH_SENDER);
        needs_saving = true;
    }
    snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "AUTH: Loaded sender: %s\r\n", g_authorized_sender);
    log_message(g_log_buffer);


    if (!eeprom_read_string(MQTT_BROKER_IP_ADDR, g_mqtt_broker_ip, sizeof(g_mqtt_broker_ip)) ||
        !isdigit((unsigned char)g_mqtt_broker_ip[0])
       )
    {
        log_message("MQTT:Saving default.\r\n");
        strcpy(g_mqtt_broker_ip, "3.109.116.92");
        needs_saving = true;
    }
    snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "MQTT: Loaded IP: %s\r\n", g_mqtt_broker_ip);
    log_message(g_log_buffer);



    if (!eeprom_read_string(MQTT_BROKER_PORT_ADDR, g_mqtt_broker_port, sizeof(g_mqtt_broker_port)) ||
        !isdigit((unsigned char)g_mqtt_broker_port[0])
       )
    {
        log_message("MQTT: No/Invalid Broker Port.\r\n");
        strcpy(g_mqtt_broker_port, "1883");
        needs_saving = true;
    }
    snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "MQTT: Loaded Port: %s\r\n", g_mqtt_broker_port);
    log_message(g_log_buffer);



    if (!eeprom_read_string(MQTT_CLIENT_ID_ADDR, g_mqtt_client_id, sizeof(g_mqtt_client_id)) || g_mqtt_client_id[0] == '\0') {
        log_message("MQTT: No/Invalid Client ID. Saving default.\r\n");
        strcpy(g_mqtt_client_id, "spring-client");
        needs_saving = true;
    }
    snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "MQTT: Loaded ClientID: %s\r\n", g_mqtt_client_id);
    log_message(g_log_buffer);



    if (!eeprom_read_string(MQTT_USERNAME_ADDR, g_mqtt_username, sizeof(g_mqtt_username)) || g_mqtt_username[0] == '\0') {
        log_message("MQTT: No/Invalid Username.\r\n");
        strcpy(g_mqtt_username, "Thinture");
        needs_saving = true;
    }
     snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "MQTT: Loaded User: %s\r\n", g_mqtt_username);
     log_message(g_log_buffer);



    if (!eeprom_read_string(MQTT_PASSWORD_ADDR, g_mqtt_password, sizeof(g_mqtt_password)) || g_mqtt_password[0] == '\0') {
        log_message("MQTT: No/Invalid Password. Saving default.\r\n");
        strcpy(g_mqtt_password, "Thinture24");
        needs_saving = true;
    }

    log_message("MQTT: Loaded Password (not displayed).\r\n");



    if (!eeprom_read_string(MQTT_TOPIC_ADDR, g_mqtt_topic, sizeof(g_mqtt_topic)) || g_mqtt_topic[0] == '\0') {
        log_message("MQTT: No/Invalid Topic. Saving default.\r\n");
        strcpy(g_mqtt_topic, "Test");
        needs_saving = true;
    }
    snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "MQTT: Loaded Topic: %s\r\n", g_mqtt_topic);
    log_message(g_log_buffer);


    if (!eeprom_read_string(SEC_MQTT_BROKER_IP_ADDR, g_secondary_mqtt_broker_ip, sizeof(g_secondary_mqtt_broker_ip)) ||
        !isdigit((unsigned char)g_secondary_mqtt_broker_ip[0]))
    {
        log_message("MQTT2: No/Invalid Broker IP. Saving default.\r\n");
        strcpy(g_secondary_mqtt_broker_ip, "43.205.58.131");
        needs_saving = true;
    }
    snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "MQTT2: Loaded IP: %s\r\n", g_secondary_mqtt_broker_ip);
    log_message(g_log_buffer);


    if (!eeprom_read_string(SEC_MQTT_BROKER_PORT_ADDR, g_secondary_mqtt_broker_port, sizeof(g_secondary_mqtt_broker_port)) ||
        !isdigit((unsigned char)g_secondary_mqtt_broker_port[0]))
    {
        log_message("MQTT2: No/Invalid Broker Port. Saving default.\r\n");
        strcpy(g_secondary_mqtt_broker_port, "1884");
        needs_saving = true;
    }
    snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "MQTT2: Loaded Port: %s\r\n", g_secondary_mqtt_broker_port);
    log_message(g_log_buffer);


    if (!eeprom_read_string(SEC_MQTT_CLIENT_ID_ADDR, g_secondary_mqtt_client_id, sizeof(g_secondary_mqtt_client_id)) || g_secondary_mqtt_client_id[0] == '\0') {
        log_message("MQTT2: No/Invalid Client ID. Saving default.\r\n");
        strcpy(g_secondary_mqtt_client_id, "spring-client-secondary");
        needs_saving = true;
    }
    snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "MQTT2: Loaded ClientID: %s\r\n", g_secondary_mqtt_client_id);
    log_message(g_log_buffer);


    if (!eeprom_read_string(SEC_MQTT_USERNAME_ADDR, g_secondary_mqtt_username, sizeof(g_secondary_mqtt_username)) || g_secondary_mqtt_username[0] == '\0') {
        log_message("MQTT2: No/Invalid Username. Saving default.\r\n");
        strcpy(g_secondary_mqtt_username, "Thinture");
        needs_saving = true;
    }
    snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "MQTT2: Loaded User: %s\r\n", g_secondary_mqtt_username);
    log_message(g_log_buffer);


    if (!eeprom_read_string(SEC_MQTT_PASSWORD_ADDR, g_secondary_mqtt_password, sizeof(g_secondary_mqtt_password)) || g_secondary_mqtt_password[0] == '\0') {
        log_message("MQTT2: No/Invalid Password. Saving default.\r\n");
        strcpy(g_secondary_mqtt_password, "Thinture");
        needs_saving = true;
    }
    log_message("MQTT2: Loaded Password (not displayed).\r\n");


    if (!eeprom_read_string(SEC_MQTT_TOPIC_ADDR, g_secondary_mqtt_topic, sizeof(g_secondary_mqtt_topic)) || g_secondary_mqtt_topic[0] == '\0') {
        log_message("MQTT2: No/Invalid Topic. Saving default.\r\n");
        strcpy(g_secondary_mqtt_topic, "Test");
        needs_saving = true;
    }
    snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "MQTT2: Loaded Topic: %s\r\n", g_secondary_mqtt_topic);
    log_message(g_log_buffer);


    if (!eeprom_read_string(DEVICE_ID_ADDR, g_device_id, sizeof(g_device_id)) || g_device_id[0] == '\0') {
        log_message("MQTT: No/Invalid Device ID. Saving default.\r\n");
        strcpy(g_device_id, "THIN0011");
        needs_saving = true;
    }
    snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "MQTT: Loaded DeviceID: %s\r\n", g_device_id);
    log_message(g_log_buffer);



    if (!eeprom_read_string(TIMEZONE_EEPROM_ADDR, g_time_zone_str, sizeof(g_time_zone_str)) ||
        (strlen(g_time_zone_str) != 6) ||
        (g_time_zone_str[0] != '+' && g_time_zone_str[0] != '-') ||
        (g_time_zone_str[3] != ':'))
    {
        log_message("TIMEZONE: No/Invalid. Saving default.\r\n");
        strcpy(g_time_zone_str, "+00:00");
        needs_saving = true;
    }
    snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "TIMEZONE: Loaded TZ: %s\r\n", g_time_zone_str);
    log_message(g_log_buffer);




    if (needs_saving) {
        log_message("default settings back to EEPROM");
        eeprom_write_string(AUTH_SENDER_EEPROM_ADDR, g_authorized_sender, AUTH_SENDER_MAX_LEN);
        eeprom_write_string(MQTT_BROKER_IP_ADDR, g_mqtt_broker_ip, sizeof(g_mqtt_broker_ip));
        eeprom_write_string(MQTT_BROKER_PORT_ADDR, g_mqtt_broker_port, sizeof(g_mqtt_broker_port));
        eeprom_write_string(MQTT_CLIENT_ID_ADDR, g_mqtt_client_id, sizeof(g_mqtt_client_id));
        eeprom_write_string(MQTT_USERNAME_ADDR, g_mqtt_username, sizeof(g_mqtt_username));
        eeprom_write_string(MQTT_PASSWORD_ADDR, g_mqtt_password, sizeof(g_mqtt_password));
        eeprom_write_string(MQTT_TOPIC_ADDR, g_mqtt_topic, sizeof(g_mqtt_topic));


        eeprom_write_string(SEC_MQTT_BROKER_IP_ADDR, g_secondary_mqtt_broker_ip, sizeof(g_secondary_mqtt_broker_ip));
        eeprom_write_string(SEC_MQTT_BROKER_PORT_ADDR, g_secondary_mqtt_broker_port, sizeof(g_secondary_mqtt_broker_port));
        eeprom_write_string(SEC_MQTT_CLIENT_ID_ADDR, g_secondary_mqtt_client_id, sizeof(g_secondary_mqtt_client_id));
        eeprom_write_string(SEC_MQTT_USERNAME_ADDR, g_secondary_mqtt_username, sizeof(g_secondary_mqtt_username));
        eeprom_write_string(SEC_MQTT_PASSWORD_ADDR, g_secondary_mqtt_password, sizeof(g_secondary_mqtt_password));
        eeprom_write_string(SEC_MQTT_TOPIC_ADDR, g_secondary_mqtt_topic, sizeof(g_secondary_mqtt_topic));


        eeprom_write_string(DEVICE_ID_ADDR, g_device_id, sizeof(g_device_id));



        eeprom_write_string(TIMEZONE_EEPROM_ADDR, g_time_zone_str, sizeof(g_time_zone_str));


        log_message("Default settings saved");
    }


    log_message("-Loaded/Validated\r\n");
}



float nmea_to_decimal(float nmea_coord, char direction) {
    if (nmea_coord == 0.0f) {
        return 0.0f;
    }
    int degrees = (int)(nmea_coord / 100.0f);
    double minutes = nmea_coord - (degrees * 100.0f);
    double decimal_degrees = degrees + (minutes / 60.0f);
    if (direction == 'S' || direction == 'W') {
        decimal_degrees = -decimal_degrees;
    }
    return (float)decimal_degrees;
}

double atof_custom(const char *s)
{
    double a = 0.0;
    int e = 0;
    int c;
    if (!s) return 0.0;
    while ((c = *s++) != '\0' && (c >= '0' && c <= '9')) {
        a = a*10.0 + (c - '0');
    }
    if (c == '.') {
        while ((c = *s++) != '\0' && (c >= '0' && c <= '9')) {
            a = a*10.0 + (c - '0');
            e = e-1;
        }
    }
    while (e < 0) {
        a *= 0.1;
        e++;
    }
    return a;
}

double haversine_distance(double lat1, double lon1, double lat2, double lon2) {
    const double R = 6371000.0; // Earth's radius in meters
    const double PI_DIV_180 = M_PI / 180.0;

    double lat1_rad = lat1 * PI_DIV_180;
    double lon1_rad = lon1 * PI_DIV_180;
    double lat2_rad = lat2 * PI_DIV_180;
    double lon2_rad = lon2 * PI_DIV_180;

    double dLat = lat2_rad - lat1_rad;
    double dLon = lon2_rad - lon1_rad;

    double a = sin(dLat / 2) * sin(dLat / 2) +
               cos(lat1_rad) * cos(lat2_rad) *
               sin(dLon / 2) * sin(dLon / 2);

    double c = 2 * atan2(sqrt(a), sqrt(1 - a));

    return R * c;
}

void configure_gps_module_lc86g(void) {
    const char* cmd;
    const uint32_t cmd_delay = 150; // ms

    log_message("\r\n--- Configuring LC86G GNSS Module ---\r\n");


    log_message("--- Performing factory reset... ---\r\n");
    cmd = "$PAIR007*3D\r\n";
    HAL_UART_Transmit(&huart12, (uint8_t*)cmd, strlen(cmd), 1000);
    vTaskDelay(pdMS_TO_TICKS(2000));


    log_message("--- Setting GNSS constellations (causes reboot) ---\r\n");
    cmd = "$PAIR066,1,1,1,1,1,0*3B\r\n";
    HAL_UART_Transmit(&huart12, (uint8_t*)cmd, strlen(cmd), 1000);
    vTaskDelay(pdMS_TO_TICKS(1000));


    log_message("--- Sending remaining configuration... ---\r\n");
    cmd = "$PAIR050,1000*12\r\n";
    HAL_UART_Transmit(&huart12, (uint8_t*)cmd, strlen(cmd), 1000);
    vTaskDelay(pdMS_TO_TICKS(cmd_delay));


    const char* disable_cmds[] = {
        "$PAIR062,1,0*3A\r\n",
        "$PAIR062,2,0*39\r\n",
        "$PAIR062,3,0*38\r\n",
        "$PAIR062,5,0*3F\r\n",
        NULL
    };
    for(int i=0; disable_cmds[i] != NULL; ++i) {
        HAL_UART_Transmit(&huart12, (uint8_t*)disable_cmds[i], strlen(disable_cmds[i]), 1000);
        vTaskDelay(pdMS_TO_TICKS(cmd_delay));
    }


    cmd = "$PAIR062,4,1*3D\r\n";
    HAL_UART_Transmit(&huart12, (uint8_t*)cmd, strlen(cmd), 1000);
    vTaskDelay(pdMS_TO_TICKS(cmd_delay));

    cmd = "$PAIR062,0,1*3B\r\n";
    HAL_UART_Transmit(&huart12, (uint8_t*)cmd, strlen(cmd), 1000);
    vTaskDelay(pdMS_TO_TICKS(cmd_delay));


    log_message("--- Saving configuration to GNSS module flash ---\r\n");
    cmd = "$PAIR513*3D\r\n";
    HAL_UART_Transmit(&huart12, (uint8_t*)cmd, strlen(cmd), 1000);
    vTaskDelay(pdMS_TO_TICKS(500));

    log_message("--- LC86G GNSS Module Configuration Sent and Saved ---\r\n\r\n");
}

int if_altitude_true=0;
float speed_kmh=0.0;
void parse_gnrmc(char* gnrmc_sentence)
{
    char temp_sentence[MAX_NMEA_SENTENCE_LEN];
    gps_data_t new_data = {0};

    strncpy(temp_sentence, gnrmc_sentence, sizeof(temp_sentence) - 1);
    temp_sentence[sizeof(temp_sentence) - 1] = '\0';

    char* fields[15] = {NULL};
    int field_count = 0;
    char *saveptr;
    char* token = strtok_r(temp_sentence, ",*", &saveptr);
    while(token != NULL && field_count < 15) {
        fields[field_count++] = token;
        token = strtok_r(NULL, ",*", &saveptr);
    }

    if (field_count >= 10 && fields[2] != NULL && fields[2][0] == 'A' &&
        fields[3] != NULL && strlen(fields[3]) > 4 &&
        fields[5] != NULL && strlen(fields[5]) > 5)
    {
        new_data.is_valid = true;


        bool just_regained_fix = false;
        if (!g_gps_fix_acquired) {
            g_gps_fix_acquired = true;
            log_message("******** GPS FIX ACQUIRED ********\r\n");
            just_regained_fix = true;
        }


		if (fields[1] != NULL) strncpy(new_data.time, fields[1], sizeof(new_data.time) - 1);
		if (fields[9] != NULL) strncpy(new_data.date, fields[9], sizeof(new_data.date) - 1);
		new_data.latitude = nmea_to_decimal(atof_custom(fields[3]), fields[4][0]);
		new_data.longitude = nmea_to_decimal(atof_custom(fields[5]), fields[6][0]);
		if (fields[7] != NULL)
		{
			new_data.speed_knots = atof_custom(fields[7]);
			speed_kmh = new_data.speed_knots * 1.852;

		}
		if (fields[8] != NULL) new_data.course_degrees = atof_custom(fields[8]);




        if (g_last_valid_gps_data.is_valid) {
            double distance_moved = haversine_distance(
                g_last_valid_gps_data.latitude, g_last_valid_gps_data.longitude,
                new_data.latitude, new_data.longitude
            );

        }

        if (fields[1] != NULL && strlen(fields[1]) >= 6 &&
            fields[9] != NULL && strlen(fields[9]) >= 6 &&
            (just_regained_fix || g_update_rtc_from_gps_flag))
        {
            log_message("RTC_SYNC: Attempting to sync RTC from GPS...\r\n");
            bool sync_success = sync_rtc_with_gps(fields[9], fields[1], g_time_zone_str);

            if (sync_success) {

                g_update_rtc_from_gps_flag = false;
                log_message("RTC_SYNC: Successfully set RTC from GPS.\r\n");
            } else {
                log_message("RTC_SYNC: Failed to parse GPS/TZ data for RTC sync.\r\n");
            }
        }


    } else {
        new_data.is_valid = false;
        if (g_gps_fix_acquired) {
             g_gps_fix_acquired = false;
             log_message("**GPS FIX LOST**\r\n");
        }
    }

    if (xSemaphoreTake(g_gps_data_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        if (new_data.is_valid) {
            new_data.altitude = g_gps_data.altitude;
            g_gps_data = new_data;
            g_last_valid_gps_data = new_data;
        } else {
            g_gps_data.is_valid = false;
        }
        xSemaphoreGive(g_gps_data_mutex);
    }
}

void parse_gngga(char* gngga_sentence) {
    char temp_sentence[MAX_NMEA_SENTENCE_LEN];
    strncpy(temp_sentence, gngga_sentence, sizeof(temp_sentence) - 1);
    temp_sentence[sizeof(temp_sentence) - 1] = '\0';

    char* fields[15] = {NULL};
    int field_count = 0;
    char *saveptr;
    char* token = strtok_r(temp_sentence, ",*", &saveptr);
    while(token != NULL && field_count < 15) {
        fields[field_count++] = token;
        token = strtok_r(NULL, ",*", &saveptr);
    }

    if (field_count >= 10 && fields[9] != NULL && strlen(fields[9]) > 0) {
        float altitude = atof_custom(fields[9]);
        if (xSemaphoreTake(g_gps_data_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        	if_altitude_true=9;
            g_gps_data.altitude = altitude;
            xSemaphoreGive(g_gps_data_mutex);

        }
    }
}

void process_gps_buffer(uint8_t* buffer, uint16_t size) {
    static char nmea_sentence[MAX_NMEA_SENTENCE_LEN];
    static uint16_t sentence_index = 0;
    if (buffer == NULL || size == 0) return;

    for (uint16_t i = 0; i < size; i++) {
        char ch = (char)buffer[i];
        if (ch == '$') {
            sentence_index = 0;
        }
        if (sentence_index < (sizeof(nmea_sentence) - 1)) {
            nmea_sentence[sentence_index++] = ch;
        }
        if (ch == '\n') {
            nmea_sentence[sentence_index] = '\0';
            if (strncmp(nmea_sentence, "$GNRMC", 6) == 0) {
                parse_gnrmc(nmea_sentence);
            } else if (strncmp(nmea_sentence, "$GNGGA", 6) == 0) {
                parse_gngga(nmea_sentence);
            }
            sentence_index = 0;
        }
    }
}


static uint8_t decToBcd(int val) {
    return (uint8_t)((val / 10 * 16) + (val % 10));
}

static int bcdToDec(uint8_t val) {
    return (int)((val / 16 * 10) + (val % 16));
}

void DS3231_SetTime(RTC_Time *time) {
    uint8_t buf[7];
    buf[0] = decToBcd(time->seconds);
    buf[1] = decToBcd(time->minutes);
    buf[2] = decToBcd(time->hour);
    buf[3] = decToBcd(time->day_of_week);
    buf[4] = decToBcd(time->day_of_month);
    buf[5] = decToBcd(time->month);
    buf[6] = decToBcd(time->year);
    HAL_I2C_Mem_Write(&hi2c4, DS3231_I2C_ADDR, DS3231_REG_SECONDS, 1, buf, 7, HAL_MAX_DELAY);
}

void DS3231_GetTime(RTC_Time *time) {
    uint8_t buf[7];
    HAL_I2C_Mem_Read(&hi2c4, DS3231_I2C_ADDR, DS3231_REG_SECONDS, 1, buf, 7, HAL_MAX_DELAY);
    time->seconds = bcdToDec(buf[0]);
    time->minutes = bcdToDec(buf[1]);
    time->hour = bcdToDec(buf[2]);
    time->day_of_week = bcdToDec(buf[3]);
    time->day_of_month = bcdToDec(buf[4]);
    time->month = bcdToDec(buf[5]);
    time->year = bcdToDec(buf[6]);
}


bool mount_sd_card_with_retry(FATFS* fatfs, uint8_t max_attempts, uint32_t retry_delay_ms) {
    FRESULT fres;
    for (uint8_t attempt = 1; attempt <= max_attempts; ++attempt) {
        snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "SD_MOUNT: Attempt %d/%d...\r\n", attempt, max_attempts);
        log_message(g_log_buffer);
        fres = f_mount(fatfs, "", 1);
        if (fres == FR_OK) {
            log_message("SD_MOUNT: Success!\r\n");
            return true;
        }
        snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "SD_MOUNT: Failed with error code %d\r\n", fres);
        log_message(g_log_buffer);
        if (attempt < max_attempts) vTaskDelay(pdMS_TO_TICKS(retry_delay_ms));
    }
    return false;
}

bool load_log_metadata(LogMetadata *metadata) {
    FIL metaFile;
    UINT bytesRead;
    if (f_open(&metaFile, METADATA_FILE, FA_READ) != FR_OK) {
        metadata->current_position = 0;
        metadata->total_entries_written = 0;
        metadata->buffer_full = 0;
        log_message("LOG_META: No metadata file found, starting fresh.\r\n");
        return false;
    }
    f_read(&metaFile, metadata, sizeof(LogMetadata), &bytesRead);
    f_close(&metaFile);
    if (bytesRead == sizeof(LogMetadata)) {
        log_message("LOG_META: Metadata loaded successfully.\r\n");
        return true;
    }

    metadata->current_position = 0;
    metadata->total_entries_written = 0;
    metadata->buffer_full = 0;
    log_message("LOG_META: Corrupted metadata file, resetting.\r\n");
    return false;
}

bool save_log_metadata(LogMetadata *metadata)
{
    FIL metaFile;
    UINT bytesWritten;
    if (f_open(&metaFile, METADATA_FILE, FA_WRITE | FA_CREATE_ALWAYS) != FR_OK) {
        log_message("LOG_META: Failed to open metadata file for writing!\r\n");
        return false;
    }
    f_write(&metaFile, metadata, sizeof(LogMetadata), &bytesWritten);
    f_close(&metaFile);
    if (bytesWritten == sizeof(LogMetadata)) {
        return true;
    }
    return false;
}



FRESULT preallocate_file(const char *filename, uint32_t size_bytes)
{
    FIL file;
    FRESULT res;


    res = f_open(&file, filename, FA_CREATE_ALWAYS | FA_WRITE);
    if (res != FR_OK)
        return res;


    res = f_lseek(&file, size_bytes - 1);
    if (res != FR_OK)
    {
        f_close(&file);
        return res;
    }


    UINT bw;
    res = f_write(&file, "", 1, &bw);
    if (res != FR_OK || bw != 1)
    {
        f_close(&file);
        return res;
    }


    f_sync(&file);

    f_close(&file);
    return FR_OK;
}

bool init_circular_log_file(FIL *logFile, LogMetadata *metadata)
{
    FRESULT fres;
    UINT bytesWritten;




    fres = f_open(logFile, DATALOG_FILE, FA_WRITE | FA_READ);
    if (fres == FR_OK)
    {
        FSIZE_t expected_size = (FSIZE_t)TOTAL_LOG_ENTRIES * MAX_LOG_ENTRY_SIZE;
        FSIZE_t actual_size = f_size(logFile);
        if (actual_size <=expected_size)
        {
            log_message("LOG_INIT: Existing log file is valid.\r\n");
            return true;
        }

        snprintf(g_log_buffer, MAX_LOG_MSG_LEN,
                 "LOG_INIT: Size mismatch! Expected=%lu, Got=%lu\r\n",
                 (unsigned long)expected_size, (unsigned long)actual_size);
        log_message(g_log_buffer);
        f_close(logFile);
        f_unlink(DATALOG_FILE);
        log_message("LOG_INIT: Recreating file...\r\n");
    }




    fres = f_open(logFile, DATALOG_FILE, FA_WRITE | FA_CREATE_NEW);
    if (fres != FR_OK)
    {
        snprintf(g_log_buffer, MAX_LOG_MSG_LEN,
                 "LOG_INIT: FAILED to create file! Error=%d\r\n", fres);
        log_message(g_log_buffer);
        return false;
    }


    char dummy_entry[MAX_LOG_ENTRY_SIZE];
    memset(dummy_entry, 0, MAX_LOG_ENTRY_SIZE);
    snprintf(dummy_entry, MAX_LOG_ENTRY_SIZE, "\r\n");

    log_message("LOG_INIT: Pre-allocating space (with error checking)...\r\n");
    UINT bwZ = 0;
//    for (uint32_t i = 0; i < TOTAL_LOG_ENTRIES; i++) {
//
//
//        // ✅ Write with error checking
//        fres = f_write(logFile, dummy_entry, MAX_LOG_ENTRY_SIZE, &bwZ);
//
//        // ✅ Check for errors
//        if (fres != FR_OK) {
//            snprintf(g_log_buffer, MAX_LOG_MSG_LEN,
//                     "LOG_INIT: Write FAILED at entry %lu! Error=%d\r\n",
//                     (unsigned long)i, fres);
//            log_message(g_log_buffer);
//            f_close(logFile);
//            f_unlink(DATALOG_FILE);
//            return false;
//        }
//
//        // ✅ Verify bytes written
//        if (bwZ != MAX_LOG_ENTRY_SIZE) {
//            snprintf(g_log_buffer, MAX_LOG_MSG_LEN,
//                     "LOG_INIT: Incomplete write at entry %lu! Expected=%d, Wrote=%u\r\n",
//                     (unsigned long)i, MAX_LOG_ENTRY_SIZE, bwZ);
//            log_message(g_log_buffer);
//            f_close(logFile);
//            f_unlink(DATALOG_FILE);
//            return false;
//        }
//
//        // ✅ Sync every 100 entries (more frequent) + yield to other tasks
//        if (i % 100 == 0) {
//            f_sync(logFile);  // Force write to SD card
//            vTaskDelay(pdMS_TO_TICKS(10));  // Give other tasks time
//
//            // Progress indicator every 1000 entries
//            if (i % 1000 == 0 && i > 0) {
//                snprintf(g_log_buffer, MAX_LOG_MSG_LEN,
//                         "LOG_INIT: Progress: %lu/%lu entries written\r\n",
//                         (unsigned long)i, (unsigned long)TOTAL_LOG_ENTRIES);
//                log_message(g_log_buffer);
//            }
//        }
//        vTaskDelay(pdMS_TO_TICKS(40));
//    }

    //preallocate_file(logFile, 51840);

    // ✅ Final sync before closing
    log_message("LOG_INIT: Syncing final data...\r\n");
    fres = f_sync(logFile);
    if (fres != FR_OK) {
        snprintf(g_log_buffer, MAX_LOG_MSG_LEN,
                 "LOG_INIT: Final sync FAILED! Error=%d\r\n", fres);
        log_message(g_log_buffer);
    }

    f_close(logFile);
    vTaskDelay(pdMS_TO_TICKS(100));  // Let SD card settle

    // ✅ Re-open and verify final size
    fres = f_open(logFile, DATALOG_FILE, FA_READ | FA_WRITE);
    if (fres != FR_OK) {
        log_message("LOG_INIT: FAILED to re-open file after creation!\r\n");
        return false;
    }

//    FSIZE_t final_size = f_size(logFile);
//    FSIZE_t expected_size = (FSIZE_t)TOTAL_LOG_ENTRIES * MAX_LOG_ENTRY_SIZE;
//
//    if (final_size != expected_size) {
//        snprintf(g_log_buffer, MAX_LOG_MSG_LEN,
//                 "LOG_INIT: VERIFICATION FAILED! Expected=%lu, Got=%lu\r\n",
//                 (unsigned long)expected_size, (unsigned long)final_size);
//        log_message(g_log_buffer);
//        f_close(logFile);
//        return false;
//    }

    log_message("LOG_INIT: New log file created and verified successfully!\r\n");

    // Reset metadata
    metadata->current_position = 0;
    metadata->total_entries_written = 0;
    metadata->buffer_full = 0;

    return true;
}

bool write_circular_log_entry(FIL *logFile, LogMetadata *metadata, const char *log_entry) {




    char padded_entry[MAX_LOG_ENTRY_SIZE];
    UINT bytesWritten;


    memset(padded_entry, ' ', MAX_LOG_ENTRY_SIZE);
    size_t entry_len = strlen(log_entry);
    if (entry_len > MAX_LOG_ENTRY_SIZE - 2) entry_len = MAX_LOG_ENTRY_SIZE - 2;
    memcpy(padded_entry, log_entry, entry_len);
    padded_entry[MAX_LOG_ENTRY_SIZE - 2] = '\r';
    padded_entry[MAX_LOG_ENTRY_SIZE - 1] = '\n';


    if (f_lseek(logFile, metadata->current_position * MAX_LOG_ENTRY_SIZE) != FR_OK){

    	return false;
    }


    if (f_write(logFile, padded_entry, MAX_LOG_ENTRY_SIZE, &bytesWritten) != FR_OK) {

    	return false;
    }



    f_sync(logFile);
    vTaskDelay(pdMS_TO_TICKS(150));
    if (bytesWritten != MAX_LOG_ENTRY_SIZE){

    	return false;
    }


    metadata->current_position++;
    metadata->total_entries_written++;
    if (metadata->current_position >= TOTAL_LOG_ENTRIES) {
        metadata->current_position = 0;
        metadata->buffer_full = 1;
    }


    return true;
}




void vLoggingTask(void *pvParameters) {
    char rx_buffer[MAX_LOG_MSG_LEN];

    log_message("--- RTOS Logging Task Optimized ---\r\n");

    for (;;) {

        if (xQueueReceive(g_log_queue, rx_buffer, portMAX_DELAY) == pdPASS) {


            HAL_UART_Transmit(&huart6, (uint8_t*)rx_buffer, strlen(rx_buffer), 100);
        }


    }
}

char alt_str[25];


void vGpsTask(void *pvParameters) {
    log_message("--- GPS Task Started. Listening on UART12... ---\r\n");

    vTaskDelay(pdMS_TO_TICKS(500));
    configure_gps_module_lc86g();

    HAL_UARTEx_ReceiveToIdle_DMA(&huart12, g_gps_dma_rx_buffer, GPS_DMA_RX_BUFFER_SIZE);

    for(;;) {

        if (xSemaphoreTake(g_gps_data_ready_sem, pdMS_TO_TICKS(350)) == pdTRUE) {
            uint16_t write_pos_local = write_pos;
            if (write_pos_local != read_pos) {
                if (write_pos_local > read_pos) {
                    process_gps_buffer(&g_gps_dma_rx_buffer[read_pos], write_pos_local - read_pos);
                } else {
                    process_gps_buffer(&g_gps_dma_rx_buffer[read_pos], GPS_DMA_RX_BUFFER_SIZE - read_pos);
                    process_gps_buffer(&g_gps_dma_rx_buffer[0], write_pos_local);
                }
            }
            read_pos = write_pos_local;
        }
        if(get_gps_fast==9)
        {
        	get_gps_fast=0;

        }
        vTaskDelay(pdMS_TO_TICKS(100));
    }
}
int speed_signal_error=0;
int gps_limp = 0;
int power_limp=0;
int vehicle_in_limp_mode=0;
char speed_string[12];
float actual_vehicle_spped=0.0;

float speed_from_abs=0.0;

char history_payload[70];
char his_time[12];






void vCircularTask(void *pvParameters) {

	uint16_t his_seq_num=0;
	char lat_str[20], lon_str[20];
	char course_str[6];
	char his_seq_str[4];

	time_t  history_utc;
    vTaskDelay(pdMS_TO_TICKS(2000));
    log_message("--- RTC & SD Card Logging Task Started ---\r\n");





    bool sd_card_available = false;
    bool sd_violation_file_ok = false;
    char log_buffer[MAX_LOG_ENTRY_SIZE];
    RTC_Time current_time;
    static uint8_t last_log_second = 99;
    uint32_t last_metadata_save_tick = 0;



    if (xSemaphoreTake(g_sd_card_mutex, portMAX_DELAY) == pdTRUE) {


    sd_card_available = mount_sd_card_with_retry(&g_fatfs, 3, 2000);
    if (sd_card_available) {
        load_log_metadata(&g_log_meta);

        if (!init_circular_log_file(&g_circular_log_file, &g_log_meta))
        {
            log_message("LOG_TASK: Failed to initialize MAIN log file!\r\n");
            sd_card_available = false;
        }

        else {
             save_log_metadata(&g_log_meta);
        }

        if (sd_card_available)
        {
            log_message("LOG_INIT: Loading violation log metadata...\r\n");
            FIL metaFile;
            UINT bytesRead;
            if (f_open(&metaFile, VIOLATION_METADATA_FILE, FA_READ) == FR_OK) {
                f_read(&metaFile, &g_violation_log_meta, sizeof(LogMetadata), &bytesRead);
                f_close(&metaFile);
                if (bytesRead == sizeof(LogMetadata)) {
                    log_message("LOG_INIT: Violation metadata loaded.\r\n");
                } else {
                    memset(&g_violation_log_meta, 0, sizeof(LogMetadata));
                    log_message("LOG_INIT: Violation metadata corrupt, resetting.\r\n");
                }
            } else {
                 memset(&g_violation_log_meta, 0, sizeof(LogMetadata));
                 log_message("LOG_INIT: No violation metadata, starting fresh.\r\n");
            }

            FRESULT fres_vio = f_open(&g_violation_log_file, VIOLATION_DATALOG_FILE, FA_WRITE | FA_READ | FA_OPEN_ALWAYS);
            if (fres_vio != FR_OK) {
                 log_message("LOG_INIT: FAILED to open/create violation log file!\r\n");
                 sd_violation_file_ok = false;
            } else {
//                 if (f_size(&g_violation_log_file) <= (FSIZE_t)TOTAL_LOG_ENTRIES * MAX_LOG_ENTRY_SIZE) {
//                     log_message("LOG_INIT: Violation file size mismatch or new file. Pre-allocating...\r\n");
//                     f_lseek(&g_violation_log_file, 0);
//                    char dummy_entry[MAX_LOG_ENTRY_SIZE];
//                    memset(dummy_entry, 0, MAX_LOG_ENTRY_SIZE);
//                    snprintf(dummy_entry, MAX_LOG_ENTRY_SIZE, "\r\n");
//                    UINT bwV;
////                    for (uint32_t i = 0; i < TOTAL_LOG_ENTRIES; i++) {
////                        f_write(&g_violation_log_file, dummy_entry, MAX_LOG_ENTRY_SIZE, &bwV);
////                        if (i % 1000 == 0) vTaskDelay(pdMS_TO_TICKS(5));
////                    }
//                    f_sync(&g_violation_log_file);
//                    g_violation_log_meta.current_position = 0;
//                    g_violation_log_meta.total_entries_written = 0;
//                    g_violation_log_meta.buffer_full = 0;
//                 }
                 log_message("LOG_INIT: Violation log file is ready.\r\n");
                 sd_violation_file_ok = true;

                 if (f_open(&metaFile, VIOLATION_METADATA_FILE, FA_WRITE | FA_CREATE_ALWAYS) == FR_OK) {
                    f_write(&metaFile, &g_violation_log_meta, sizeof(LogMetadata), &bytesRead);
                    f_close(&metaFile);
                 }
            }
        }
    }





    xSemaphoreGive(g_sd_card_mutex);



}













    int k=1;

	HAL_GPIO_WritePin(OUTPUT_BUZZER_GPIO_Port, OUTPUT_BUZZER_Pin, GPIO_PIN_SET);
	vTaskDelay(pdMS_TO_TICKS(2500));
	HAL_GPIO_WritePin(OUTPUT_BUZZER_GPIO_Port, OUTPUT_BUZZER_Pin, GPIO_PIN_RESET);
	vTaskDelay(pdMS_TO_TICKS(100));

    for(;;) {
        DS3231_GetTime(&current_time);


        if (g_gps_data.is_valid)
        {
            gps_speed_kmhr = speed_kmh;
            if(gps_speed_kmhr > 3)
            {
                if(speed_from_abs < 1) {
                    speed_signal_error = 9;
                    gps_limp = 9;
                    sprintf(signal_status,"SigD");
                    vehicle_in_limp_mode = 9;
                    actual_vehicle_spped = gps_speed_kmhr;
                    speed_rng = gps_speed_kmhr;
                    g_log_violation_event = true;
                    his_signal_cut=0;
                } else {
                    vehicle_in_limp_mode = 0;
                    gps_limp = 0;
                    speed_signal_error = 0;
                    sprintf(signal_status,"SigC");
                    actual_vehicle_spped = speed_rng;
                    his_signal_cut=1;
                }
            }
            else
            {
            	vehicle_in_limp_mode = 0;
				gps_limp = 0;
				speed_signal_error = 0;
				sprintf(signal_status,"SigC");
				actual_vehicle_spped = speed_rng;
				his_signal_cut=1;
            }
        }
        else
        {
            gps_speed_kmhr = 0;
            vehicle_in_limp_mode = 0;
            gps_limp = 0;
            speed_signal_error = 0;
            sprintf(signal_status,"SigC");
            actual_vehicle_spped = speed_rng;
            his_signal_cut=1;
        }


        if ((current_time.seconds % LOG_INTERVAL_SECONDS == 0) && (current_time.seconds != last_log_second)) {
        	log_message("circular task\r\n");
            last_log_second = current_time.seconds;

            snprintf(g_log_buffer, MAX_LOG_MSG_LEN, "Power=%d-->%d\r\n", power_input, ignition_input);
            log_message(g_log_buffer);



            master_log_payload_t master_payload = {0};



            master_payload.rtc_time = current_time;

            if (xSemaphoreTake(g_gps_data_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
                master_payload.gps_snapshot = g_gps_data;
                xSemaphoreGive(g_gps_data_mutex);
            }



    		power_input = HAL_GPIO_ReadPin(POWERCUT_IN_GPIO_Port, POWERCUT_IN_Pin);
    		if(power_input == 1) {
    			power_limp = 9;
    			sprintf(power_status,"PowD");
    		} else {
    			power_limp = 0;
    			sprintf(power_status,"PowC");
    		}



            strcpy(master_payload.power_status, power_status);
            strcpy(master_payload.signal_status, signal_status);
            master_payload.final_log_speed = actual_vehicle_spped;
            master_payload.msld_speed = speed_rng;


            if(master_payload.gps_snapshot.is_valid && master_payload.gps_snapshot.speed_knots < IDLE_SPEED_THRESHOLD_KNOTS) {
                strcpy(master_payload.vehicle_status, "IDLE");
                his_vehicle_status = 0;
            } else {
                strcpy(master_payload.vehicle_status, "RUNNING");
                his_vehicle_status = 1;
            }


            sprintf(speed_string,"%0.1f", master_payload.final_log_speed);

            if (master_payload.gps_snapshot.is_valid) {
                snprintf(log_buffer, sizeof(log_buffer),
                         "20%02d/%02d/%02d %02d:%02d:%02d %s %.5f %.5f %s %s",
                         master_payload.rtc_time.year, master_payload.rtc_time.month, master_payload.rtc_time.day_of_month,
                         master_payload.rtc_time.hour, master_payload.rtc_time.minutes, master_payload.rtc_time.seconds,
                         speed_string, master_payload.gps_snapshot.latitude, master_payload.gps_snapshot.longitude,
                         master_payload.power_status, master_payload.signal_status);


                rtc_to_utc_timestamp(master_payload.rtc_time);


                format_coord(lat_str, sizeof(lat_str), master_payload.gps_snapshot.latitude, 'N', 'S');
                format_coord(lon_str, sizeof(lon_str), master_payload.gps_snapshot.longitude, 'E', 'W');
                snprintf(course_str, sizeof(course_str), "%03d", (int)roundf(master_payload.gps_snapshot.course_degrees));



                if(!g_gsm_connection_acquired)
                {
                	convert_sequence(his_seq_num, his_seq_str,99);
                	snprintf(history_payload, sizeof(history_payload),
                						 "%s,"
                	                	 "%s,%s,%s,%s,"
                	                	 "%d,%d,%d,%d,%s,#",
                						 his_time,
                						 lat_str, lon_str, speed_string, course_str,
                						 his_vehicle_status,his_ign_status,
                						 his_signal_cut,his_power_cut,his_seq_str);

					log_message("DataHistory\n");
					log_DATA(history_payload);
					his_seq_num++;
					if(his_seq_num>=25000)
					{
						his_seq_num=0;
					}
                }
                else
                {
                	his_seq_num=0;
                }

            } else {

                snprintf(log_buffer, sizeof(log_buffer),
                         "20%02d/%02d/%02d %02d:%02d:%02d %s SL SL %s %s",
                         master_payload.rtc_time.year, master_payload.rtc_time.month, master_payload.rtc_time.day_of_month,
                         master_payload.rtc_time.hour, master_payload.rtc_time.minutes, master_payload.rtc_time.seconds,
                         speed_string, master_payload.power_status, master_payload.signal_status);



//                rtc_to_utc_timestamp(master_payload.rtc_time);
//				//sprintf(eeprom_TRANSFER,"UTC Timestamp = %s\n", his_time);
//				//log_message(eeprom_TRANSFER);
//
//                format_coord(lat_str, sizeof(lat_str), master_payload.gps_snapshot.latitude, 'N', 'S');
//                format_coord(lon_str, sizeof(lon_str), master_payload.gps_snapshot.longitude, 'E', 'W');
//                snprintf(course_str, sizeof(course_str), "%03d", (int)roundf(master_payload.gps_snapshot.course_degrees));
//
//                snprintf(history_payload, sizeof(history_payload),
//					 "%s,N2,"
//                	 "%s,%s,%s,%s,"
//                	 "%d,%d,%d,%d#",
//					 his_time,
//					 lat_str, lon_str, speed_string, course_str,
//					 his_vehicle_status,his_ign_status,
//					 his_signal_cut,his_power_cut);
//
//               // log_message("HIS_DATA=");
//               // log_message(history_payload);
//               // log_message("\n");
//
//                //log_message("DataHistory\n");
//                //log_DATA(history_payload);

            }

            // --- 5. WRITE TO SD CARD ---
            if(sd_card_busy_printing != 9)
            {


            	if (xSemaphoreTake(g_sd_card_mutex, pdMS_TO_TICKS(100)) == pdTRUE)
            	                {

				if (sd_card_available) {
					log_message(log_buffer);
					if (!write_circular_log_entry(&g_circular_log_file, &g_log_meta, log_buffer)) {
						log_message("LOG_ERROR: Write to MAIN SD failed!\r\n");
						sd_card_available = false;
						f_close(&g_circular_log_file);
						if(sd_violation_file_ok) {
							f_close(&g_violation_log_file);
							sd_violation_file_ok = false;
						}
					}
				}

                if (g_log_violation_event && sd_violation_file_ok) {
                    g_log_violation_event = false;
                    log_message("--- VIOLATION EVENT DETECTED --- Writing to violation log...\r\n");
                    if (!write_circular_log_entry(&g_violation_log_file, &g_violation_log_meta, log_buffer)) {
                        log_message("LOG_ERROR: Write to VIOLATION SD failed!\r\n");
                        sd_violation_file_ok = false;
                        f_close(&g_violation_log_file);
                    } else {
                        FIL metaFile;
                        UINT bytesWritten;
                        if (f_open(&metaFile, VIOLATION_METADATA_FILE, FA_WRITE | FA_CREATE_ALWAYS) == FR_OK) {
                            f_write(&metaFile, &g_violation_log_meta, sizeof(LogMetadata), &bytesWritten);
                            f_close(&metaFile);
                        }
                    }
                }




                xSemaphoreGive(g_sd_card_mutex);
                                }


            	else {
            	                    log_message("SD BUSY: Skipped log write\r\n");
            	                }

            }


            xQueueOverwrite(g_data_snapshot_queue, &master_payload);


            if (HAL_GetTick() - last_metadata_save_tick > 120000) {


            	if (xSemaphoreTake(g_sd_card_mutex, pdMS_TO_TICKS(1000)) == pdTRUE)
            	                {


                 if (sd_card_available) {
                    f_sync(&g_circular_log_file);
                    save_log_metadata(&g_log_meta);
                }
                if (sd_violation_file_ok) {
                     f_sync(&g_violation_log_file);
                     FIL metaFile;
                     UINT bytesWritten;
                     if (f_open(&metaFile, VIOLATION_METADATA_FILE, FA_WRITE | FA_CREATE_ALWAYS) == FR_OK) {
                        f_write(&metaFile, &g_violation_log_meta, sizeof(LogMetadata), &bytesWritten);
                        f_close(&metaFile);
                     }
                }

                xSemaphoreGive(g_sd_card_mutex);


                if (sd_card_available || sd_violation_file_ok) {
                    log_message("LOG_META: Periodic metadata sync complete.\r\n");
                    last_metadata_save_tick = HAL_GetTick();
                }
            }
        }
        }
        vTaskDelay(pdMS_TO_TICKS(100));
    }
}




uint8_t history_lat[19];
uint8_t history_lon[19];
uint8_t history_alt[19];

uint8_t firstPart[25];
char *hashPos;

uint8_t trigger_gps_fix[100];

void apply_timezone_offset(int *year, int *month, int *day, int *hour, int *minute, int tz_hour_offset, int tz_min_offset)
{

    *minute += tz_min_offset;
    while (*minute >= 60) {
        *minute -= 60;
        (*hour)++;
    }
    while (*minute < 0) {
        *minute += 60;
        (*hour)--;
    }


    *hour += tz_hour_offset;
    bool date_changed_up = false;
    bool date_changed_down = false;

    while (*hour >= 24) {
        *hour -= 24;
        date_changed_up = true;
    }
    while (*hour < 0) {
        *hour += 24;
        date_changed_down = true;
    }


    const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int current_year_full = *year + 2000;

    if (date_changed_up) {

        int month_days = days_in_month[*month];
        if (*month == 2 && ((current_year_full % 4 == 0 && current_year_full % 100 != 0) || (current_year_full % 400 == 0))) {
            month_days = 29;
        }

        (*day)++;
        if (*day > month_days) {
            *day = 1;
            (*month)++;
            if (*month > 12) {
                *month = 1;
                (*year)++;
                if (*year > 99) *year = 0;
            }
        }
    } else if (date_changed_down) {

        (*day)--;
        if (*day < 1) {
            (*month)--;
            if (*month < 1) {
                *month = 12;
                (*year)--;
                if (*year < 0) *year = 99;
            }


            int prev_month_days = days_in_month[*month];

            int prev_year_full = (*month == 12) ? current_year_full - 1 : current_year_full;

            if (*month == 2 && ((prev_year_full % 4 == 0 && prev_year_full % 100 != 0) || (prev_year_full % 400 == 0))) {
                prev_month_days = 29;
            }
            *day = prev_month_days;
        }
    }
}


bool sync_rtc_with_gps(const char* utc_date_str, const char* utc_time_str, const char* tz_str)
{

    if (strlen(utc_date_str) < 6 || strlen(utc_time_str) < 6 || strlen(tz_str) < 6) {
        log_message("RTC_SYNC: Invalid input data length.\r\n");
        return false;
    }


    int tz_hour = 0, tz_min = 0;
    if (sscanf(tz_str, "%3d:%2d", &tz_hour, &tz_min) != 2) {
        log_message("RTC_SYNC: Failed to parse timezone string.\r\n");
        return false;
    }


    if (tz_hour < 0) {
        tz_min = -tz_min;
    }


    int day, month, year, hour, minute, second;


    day = (utc_date_str[0] - '0') * 10 + (utc_date_str[1] - '0');
    month = (utc_date_str[2] - '0') * 10 + (utc_date_str[3] - '0');
    year = (utc_date_str[4] - '0') * 10 + (utc_date_str[5] - '0');


    hour = (utc_time_str[0] - '0') * 10 + (utc_time_str[1] - '0');
    minute = (utc_time_str[2] - '0') * 10 + (utc_time_str[3] - '0');
    second = (utc_time_str[4] - '0') * 10 + (utc_time_str[5] - '0');


    if (month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || minute > 59 || second > 59) {
        log_message("RTC_SYNC: Invalid GPS date/time data.\r\n");
        return false;
    }


    apply_timezone_offset(&year, &month, &day, &hour, &minute, tz_hour, tz_min);


    RTC_Time new_rtc_time;
    new_rtc_time.seconds = (uint8_t)second;
    new_rtc_time.minutes = (uint8_t)minute;
    new_rtc_time.hour = (uint8_t)hour;
    new_rtc_time.day_of_month = (uint8_t)day;
    new_rtc_time.month = (uint8_t)month;
    new_rtc_time.year = (uint8_t)year;
    new_rtc_time.day_of_week = 1;

    DS3231_SetTime(&new_rtc_time);

    return true;
}


int parse(char data[25])
{

    hashPos = strchr(data, '#');
    if (hashPos != NULL)
    {

        size_t len = hashPos - data;

        strncpy(firstPart, data, len);
        firstPart[len] = '\0';
        return 1;
    }
    else
        return 0;

}

void write_gps_value_to_eeprom(uint16_t address, float value)
{
	int i=0;
	uint8_t buffer_data[19];

    sprintf(buffer_data, "%f#", value);
    for(i=0;i<strlen(buffer_data);i++)
    {
    	eeprom_WRITEBYTE((address+i),buffer_data[i]);
    }
    buffer_data[i]='\0';
	log_message("{");
	log_message(buffer_data);
	log_message("}");
    vTaskDelay(pdMS_TO_TICKS(2));
}

void read_gps_value_from_eeprom(uint16_t address, uint8_t *output)
{

    for(int k=0;k<19;k++)
    {
    	output[k]=eeprom_READBYTE(address+k);
    	if(output[k]=='#')
    	{
    		output[k]='\0';
    		k=99;
    	}
    }

    log_message("{");
    log_message(output);
    log_message("}");

}

unsigned int calculate_checksum( char *sentence)
{
    unsigned int checksum = 0;

    if (*sentence == '$') {
        sentence++;
    }

    while (*sentence && *sentence != '*') {
        checksum ^= (unsigned int)(*sentence);
        sentence++;
    }

    return checksum;
}
void USB_ForceReenumeration(void);

uint16_t code_delay=0;
int limbp_timimg=3;

void vStatusLedTask(void *pvParameters)
{
	int parse_return=0;
	char buffer_data[30];
	uint16_t first_gps_fix=300;
	uint16_t gps_store_eeprom=0;
	int loop_counter=0;
	int serial_set_speed_flag=0;

	uint16_t serial_speed_counter=0;
	int gps_ever_disconnected=0;
	uint16_t gps_string=0;
	int test_flg=0;

	vTaskDelay(pdMS_TO_TICKS(700));

	int k=250;
	for(;;)
	{
		k++;
		if(k>=300)
		{
			k=0;

		}
		loop_counter=loop_counter+1;

		if(loop_counter>=5)
		{
			if((power_limp==9)||(gps_limp==9))
			{
				HAL_GPIO_WritePin(POWER_LED_GPIO_Port, POWER_LED_Pin,GPIO_PIN_SET);
			}
			else
			{
				HAL_GPIO_TogglePin(POWER_LED_GPIO_Port, POWER_LED_Pin);
			}
			loop_counter=0;
			if (g_gps_fix_acquired)
			{
				test_flg=20;
				if(gps_string==0)
					gps_string=9;
				HAL_GPIO_WritePin(GPS_INDICATION_GPIO_Port, GPS_INDICATION_Pin, GPIO_PIN_SET);
			}
			else
			{
				gps_speed_kmhr=0;
				test_flg=40;
				gps_string=0;
				if_altitude_true=0;
				gps_ever_disconnected=9;
				HAL_GPIO_TogglePin(GPS_INDICATION_GPIO_Port, GPS_INDICATION_Pin);
			}
			if (g_gsm_connection_acquired)
			{
				HAL_GPIO_WritePin(GSM_INDICATION_GPIO_Port, GSM_INDICATION_Pin, GPIO_PIN_SET);
			}
			else
			{
				HAL_GPIO_TogglePin(GSM_INDICATION_GPIO_Port, GSM_INDICATION_Pin);
			}
		}

		timer_counter_ivms++;
		if(timer_counter_ivms > 2000)
		{
			timer_counter_ivms = 50;
		}

		if(serial_speed_receive.st_CFlg==1)
		{
			serial_speed_receive.st_counter++;
			if(serial_speed_receive.st_counter > NO_SERIAL_DATA_TIME)
			{
				serial_speed_receive.st_SFlg=1;
				serial_speed_receive.st_CFlg=0;
				serial_speed_receive.st_counter=0;
				serial_data_true=0;
			}
		}
		else
		{
			serial_speed_receive.st_SFlg=0;
			serial_speed_receive.st_counter=0;
		}

		if(stack_size_tester.st_CFlg==1)
		{
			stack_size_tester.st_counter++;
			if(stack_size_tester.st_counter> 20)
			{
				stack_size_tester.st_SFlg=1;
				stack_size_tester.st_CFlg=0;
				stack_size_tester.st_counter=0;
			}
		}
		else
		{
			stack_size_tester.st_SFlg=0;
			stack_size_tester.st_counter=0;
		}

		if(control_buzz_onoff.st_CFlg==1)
		{
			control_buzz_onoff.st_counter++;
			if(control_buzz_onoff.st_counter> 3)
			{
				control_buzz_onoff.st_SFlg=1;

				control_buzz_onoff.st_counter=0;
			}
		}
		else
		{
			control_buzz_onoff.st_SFlg=0;
			control_buzz_onoff.st_counter=0;
		}

		if(pid_pedal_exit.st_CFlg==1)
		{
			pid_pedal_exit.st_counter++;

			if(pid_pedal_exit.st_counter > 4)
			{
				pid_pedal_exit.st_SFlg=1;

				pid_pedal_exit.st_counter=0;
			}
		}
		else
		{
			pid_pedal_exit.st_SFlg=0;
			pid_pedal_exit.st_counter=0;
		}

		if(signal_disconnection.st_CFlg==1)
		{
			signal_disconnection.st_counter++;

			if(signal_disconnection.st_counter> 25)
			{
				signal_disconnection.st_SFlg=1;

				signal_disconnection.st_counter=0;
			}
		}
		else
		{
			signal_disconnection.st_SFlg=0;
			signal_disconnection.st_counter=0;
		}

		if(limb_control.st_CFlg==1)
		{
			limb_control.st_counter++;
			if(limb_control.st_counter>limbp_timimg)
			{
				limb_control.st_counter=0;
				limb_control.st_SFlg=1;
			}
			code_delay++;
			if(code_delay>1000)
			{
				code_delay=0;
			}
		}
		else
		{
			code_delay=0;
			limb_control.st_SFlg=0;
			limb_control.st_counter=0;
		}

		power_input 	= HAL_GPIO_ReadPin(POWERCUT_IN_GPIO_Port, POWERCUT_IN_Pin);
		ignition_input	= HAL_GPIO_ReadPin(IGNITION_IN_GPIO_Port, IGNITION_IN_Pin);



		vTaskDelay(pdMS_TO_TICKS(100));
	}
}


void perform_modem_power_cycle(void) {
    char msg[80];
    snprintf(msg, sizeof(msg), "--- Performing Modem Power Cycle on %s Pin %d ---\r\n", "GPIOB", 9);
    log_message(msg);

    log_message("Powering modem OFF...\r\n");
    HAL_GPIO_WritePin(MODEM_PWR_RST_GPIO_Port, MODEM_PWR_RST_Pin, GPIO_PIN_RESET);
    vTaskDelay(pdMS_TO_TICKS(2000));
    log_message("Powering modem ON...\r\n");
    HAL_GPIO_WritePin(MODEM_PWR_RST_GPIO_Port, MODEM_PWR_RST_Pin, GPIO_PIN_SET);
    log_message("--- Modem Power Cycle Complete. Waiting for boot... ---\r\n");
    vTaskDelay(pdMS_TO_TICKS(12000));
}


bool send_and_wait_for_response(const char* cmd, const char* expected_response, uint32_t timeout_ms) {


    if (xSemaphoreTake(g_modem_mutex, pdMS_TO_TICKS(15000)) != pdTRUE) {
        log_message("ERROR: Modem Mutex Timeout\r\n");
        return false;
    }

    uint8_t rx_buffer[MODEM_UART_RX_BUFFER_SIZE + 1];
    char log_buf[300];

    xQueueReset(g_modem_uart_rx_queue);
    snprintf(log_buf, sizeof(log_buf), "CMD >> %s\r\n", cmd);
    log_message(log_buf);
    HAL_UART_Transmit(&huart1, (uint8_t*)cmd, strlen(cmd), 1000);

    TickType_t start_ticks = xTaskGetTickCount();
    while ((xTaskGetTickCount() - start_ticks) < pdMS_TO_TICKS(timeout_ms)) {
        if (xQueueReceive(g_modem_uart_rx_queue, &rx_buffer, pdMS_TO_TICKS(100)) == pdPASS) {
            if (strlen((char*)rx_buffer) < 250) {
                 snprintf(log_buf, sizeof(log_buf), "RSP << %s\r\n", (char*)rx_buffer);
                 log_message(log_buf);
            }
            if (strstr((char*)rx_buffer, expected_response) != NULL) {
                xSemaphoreGive(g_modem_mutex);
                return true;
            }
        }
    }
    snprintf(log_buf, sizeof(log_buf), "ERROR: Timeout waiting for '%s'\r\n", expected_response);
    log_message(log_buf);
    xSemaphoreGive(g_modem_mutex);
    return false;
}

void convert_utc_datetime_to_ist(const char* utc_date_str, const char* utc_time_str, char* ist_buffer, size_t buffer_size) {
    if (utc_date_str == NULL || strlen(utc_date_str) < 6 || utc_time_str == NULL || strlen(utc_time_str) < 6) {
        snprintf(ist_buffer, buffer_size, "2025-01-01 00:00:00");
        return;
    }
    int day = (utc_date_str[0] - '0') * 10 + (utc_date_str[1] - '0');
    int month = (utc_date_str[2] - '0') * 10 + (utc_date_str[3] - '0');
    int year = 2000 + (utc_date_str[4] - '0') * 10 + (utc_date_str[5] - '0');
    int hour = (utc_time_str[0] - '0') * 10 + (utc_time_str[1] - '0');
    int minute = (utc_time_str[2] - '0') * 10 + (utc_time_str[3] - '0');
    int second = (utc_time_str[4] - '0') * 10 + (utc_time_str[5] - '0');

    hour += 5;
    minute += 30;

    if (minute >= 60) { minute -= 60; hour++; }
    bool date_changed = false;
    if (hour >= 24) { hour -= 24; date_changed = true; }

    if (date_changed) {
        day++;
        const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        int month_days = days_in_month[month];
        if (month == 2 && ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))) { month_days = 29; }
        if (day > month_days) { day = 1; month++; if (month > 12) { month = 1; year++; } }
    }
    snprintf(ist_buffer, buffer_size, "%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
}

void format_coord(char* buffer, size_t buffer_size, float coord, char positive_dir, char negative_dir) {
    char dir = (coord >= 0) ? positive_dir : negative_dir;
    float coord_abs = fabsf(coord);
    int degrees = (int)coord_abs;
    unsigned long long frac_ll = (unsigned long long)((coord_abs - degrees) * 10000000.0);
    if (frac_ll > 9999999) frac_ll = 9999999;
    unsigned long frac = (unsigned long)frac_ll;
    snprintf(buffer, buffer_size, "%03d.%07lu%c", degrees, frac, dir);
}

char g_device_imei[20] = "00000000000000000";

bool get_modem_imei(void)
{
	int imei_start=0;
    char *start = NULL;
    char *end = NULL;

    if (xSemaphoreTake(g_modem_mutex, pdMS_TO_TICKS(15000)) != pdTRUE) {
        log_message("IMEI: Mutex Timeout\r\n");
        return false;
    }

    uint8_t rx_buffer[MODEM_UART_RX_BUFFER_SIZE + 1];
    char log_buf[80];


    xQueueReset(g_modem_uart_rx_queue);

    log_message("CMD >> AT+CGSN\r\n");
    HAL_UART_Transmit(&huart1, (uint8_t*)"AT+CGSN\r\n", strlen("AT+CGSN\r\n"), 1000);

    bool imei_found = false;
    TickType_t start_ticks = xTaskGetTickCount();


    while ((xTaskGetTickCount() - start_ticks) < pdMS_TO_TICKS(5000)) {
        if (xQueueReceive(g_modem_uart_rx_queue, &rx_buffer, pdMS_TO_TICKS(200)) == pdPASS) {


        	int len = strlen((char*)rx_buffer);
            snprintf(log_buf, sizeof(log_buf), "RSP <<-%s\r\n,%u\n", (char*)rx_buffer,len);
            log_message(log_buf);


            unsigned char *p = rx_buffer;
            if (len>=15)
            {

                while (*p && !isdigit((unsigned char)*p))
                    p++;
                start = p;

                while (*p && isdigit((unsigned char)*p))
                    p++;
                end = p;
                int len = end - start;
                strncpy(g_device_imei, start, len);
                g_device_imei[len] = '\0';
                imei_found = true;
            }


            if (strstr((char*)rx_buffer, "OK") != NULL) {
                break;
            }
        }
    }


    xSemaphoreGive(g_modem_mutex);

    if (imei_found) {
        sprintf(log_buf,"IMEI Acquired:<%s>\r\n", g_device_imei);
        log_message(log_buf);
    } else {
        log_message("!!! FAILED to acquire IMEI !!!\r\n");
    }
    return imei_found;
}


extern uint8_t  to_COUNTL;
extern uint8_t  to_COUNTH;
extern uint16_t to_COUNT;

void rtc_to_utc_timestamp(RTC_Time timeDate)
{
    struct tm tm_time;

    tm_time.tm_year = timeDate.year+100;
    tm_time.tm_mon  = timeDate.month-1;
    tm_time.tm_mday = timeDate.day_of_month;

    tm_time.tm_hour = timeDate.hour;
    tm_time.tm_min  = timeDate.minutes;
    tm_time.tm_sec  = timeDate.seconds;

    time_t local_ts = mktime(&tm_time);
    sprintf(his_time,"%ld",local_ts);
}

void format_timestamp_fixed_offset(time_t ts, int offset_seconds, char *buf, size_t bufsize)
{
    time_t adj = ts + offset_seconds;
    struct tm tm_local;
    gmtime_r(&adj, &tm_local);
    strftime(buf, bufsize, "%Y-%m-%d %H:%M:%S", &tm_local);
}

char formatting_his_payload[512];
void packetize_history_data()
{
	char vehicle_stat[10];
	char vehicle_ign[7];
	char power_stat[7];
	char signal_stat[7];
	char date_time_str[22];

	if(parsed_data[5][0]=='1')
	{
		sprintf(vehicle_stat,"%s","RUNNING");
	}
	else if(parsed_data[5][0]=='0')
	{
		sprintf(vehicle_stat,"%s","IDLE");
	}

	if(parsed_data[6][0]=='1')
	{
		sprintf(vehicle_ign,"%s","IGon");
	}
	else if(parsed_data[6][0]=='0')
	{
		sprintf(vehicle_ign,"%s","IGoff");
	}

	if(parsed_data[7][0]=='1')
	{
		sprintf(signal_stat,"%s","SigC");
	}
	else if(parsed_data[7][0]=='0')
	{
		sprintf(signal_stat,"%s","SigD");
	}

	if(parsed_data[8][0]=='1')
	{
		sprintf(power_stat,"%s","PowC");
	}
	else if(parsed_data[8][0]=='0')
	{
		sprintf(power_stat,"%s","PowD");
	}

	time_t ts = (time_t)atoll(parsed_data[0]);
	format_timestamp_fixed_offset(ts,0, date_time_str, sizeof(date_time_str));


    snprintf(formatting_his_payload, sizeof(formatting_his_payload),
		 "{\"deviceID\":\"%s\",\"IMEI\":\"%s\",\"timestamp\":\"%s\","
		 "\"dataValidity\":\"Valid\",\"status\":\"N2\",\"latitude\":\"%s\","
		 "\"longitude\":\"%s\",\"speed\":\"%s\",\"course\":\"%s\","
		 "\"ignition\":\"%s\",\"vehicleStatus\":\"%s\","
		 "\"additionalData\":\"0000000000000\",\"timeIntervals\":\"002,010,002\","
		 "\"angleInterval\":\"015\",\"distanceInterval\":\"100\","
		 "\"gsmStrength\":\"073\",\"sequenceNumber\":\"%s\","
		 "\"msldSpeed_kmh\":%s,"
         "\"powerStatus\":\"%s\","
         "\"signalStatus\":\"%s\"}",
		 g_device_id, g_device_imei, date_time_str,
		 parsed_data[1], parsed_data[2], parsed_data[3], parsed_data[4],
		 vehicle_ign, vehicle_stat, // Use from payload
		 parsed_data[9],
		 parsed_data[3],
         power_stat,    // Use from payload
         signal_stat);  // Use from payload

    log_message(formatting_his_payload);
}

void convert_sequence(uint16_t n, char *out,uint16_t block_size_val)
{
    // There are 999 numbers for each letter
    const uint16_t block_size = block_size_val;

    // Wrap around after Z
    n = (n - 1) % (26 * block_size);

    uint16_t letter_index = n / block_size;       // 0 = A, 1 = B, ... 25 = Z
    uint16_t number_part = (n % block_size) + 1;  // 1..999

    char letter = 'A' + letter_index;
    if(block_size_val==999)
    	sprintf(out, "%c%03d", letter, number_part);
    else if(block_size_val==99)
    	sprintf(out, "%c%02d", letter, number_part);
}

uint16_t seq_number_long=0;
char seq_number[6];
int his_primary_connected=0;
int his_secondary_connected=0;



void vMqttTask(void *pvParameters) {

	int pdp_error_=0;
	int backup_UPLOAD=0;
	int store_history=0;
	int return_dat=0;
    log_message("--- MQTT Task Started ---\r\n");

    char command_buffer[256];
    char mqtt_payload[512];

    uint8_t primary_connection_retries = 0;
    const uint8_t max_retries_before_halt = 5;

    bool is_primary_connected = false;
    #if USE_SECONDARY_SERVER
    bool is_secondary_connected = false;
    #endif

    g_session_start_time = HAL_GetTick();
    vTaskDelay(pdMS_TO_TICKS(5000));
    HAL_UARTEx_ReceiveToIdle_DMA(&huart1, g_modem_dma_rx_buffer, MODEM_UART_RX_BUFFER_SIZE);
    perform_modem_power_cycle();

    log_message("Waiting for modem to respond...\r\n");
    while(!send_and_wait_for_response("AT\r\n", "OK", 1000)) { vTaskDelay(pdMS_TO_TICKS(1000)); }
    send_and_wait_for_response("ATE0\r\n", "OK", 1000);
    log_message("Modem is responsive.\r\n");

    log_message("--- Acquiring Modem IMEI ---\r\n");
    return_dat = get_modem_imei();


    log_message("--- Configuring Network Data Connection (PDP Context) ---\r\n");
    bool pdp_ready = false;
    seq_number_long=1;
    while (!pdp_ready) {
        if (send_and_wait_for_response("AT+CGATT=1\r\n", "OK", 8000)) {
            vTaskDelay(pdMS_TO_TICKS(1000));
            send_and_wait_for_response("AT+CGACT=0,1\r\n", "OK", 8000);
            vTaskDelay(pdMS_TO_TICKS(1000));
            snprintf(command_buffer, sizeof(command_buffer), "AT+CGDCONT=1,\"IP\",\"%s\"\r\n", g_apn);
            if (send_and_wait_for_response(command_buffer, "OK", 5000)) {
                if (send_and_wait_for_response("AT+CGACT=1,1\r\n", "OK", 60000)) {
                    log_message("--- Network Data Connection is Active ---\r\n");
                    pdp_ready = true;
                }
            }
        }
        if (!pdp_ready) {
            log_message("!!! Network Data Connection Failed. Retrying... !!!\r\n");
            vTaskDelay(pdMS_TO_TICKS(5000));
        }
    }

    while(1) {

        if (!is_primary_connected) {
            log_message("--- MQTT Primary Connection Attempt ---\r\n");
            bool setup_ok = false;
            send_and_wait_for_response("AT+QMTCLOSE=0\r\n", "OK", 5000);
            vTaskDelay(pdMS_TO_TICKS(500));
            snprintf(command_buffer, sizeof(command_buffer), "AT+QMTOPEN=0,\"%s\",%s\r\n", g_mqtt_broker_ip, g_mqtt_broker_port);
            if (send_and_wait_for_response(command_buffer, "+QMTOPEN: 0,0", 20000)) {
                vTaskDelay(pdMS_TO_TICKS(1000));
                snprintf(command_buffer, sizeof(command_buffer), "AT+QMTCONN=0,\"%s\",\"%s\",\"%s\"\r\n", g_mqtt_client_id, g_mqtt_username, g_mqtt_password);
                if (send_and_wait_for_response(command_buffer, "+QMTCONN: 0,0,0", 20000)) {
                    setup_ok = true;
                }
            }
            if (setup_ok) {
                is_primary_connected = true;
                primary_connection_retries = 0;
                log_message("--- MQTT Primary Connection Successful ---\r\n");
            } else {
                log_message("--- MQTT Primary Connection Failed ---\r\n");
                if (++primary_connection_retries >= max_retries_before_halt) {
                    perform_modem_power_cycle();
                    pdp_ready = false; continue;
                }
            }
        }
        g_gsm_connection_acquired = is_primary_connected;


        #if USE_SECONDARY_SERVER
        if (!is_secondary_connected) {
            log_message("--- MQTT Secondary Connection Attempt ---\r\n");
            send_and_wait_for_response("AT+QMTCLOSE=1\r\n", "OK", 5000);
            vTaskDelay(pdMS_TO_TICKS(500));
            snprintf(command_buffer, sizeof(command_buffer), "AT+QMTOPEN=1,\"%s\",%s\r\n", g_secondary_mqtt_broker_ip, g_secondary_mqtt_broker_port);
            if (send_and_wait_for_response(command_buffer, "+QMTOPEN: 1,0", 20000)) {
                vTaskDelay(pdMS_TO_TICKS(1000));
                snprintf(command_buffer, sizeof(command_buffer), "AT+QMTCONN=1,\"%s\",\"%s\",\"%s\"\r\n", g_secondary_mqtt_client_id, g_secondary_mqtt_username, g_secondary_mqtt_password);
                if (send_and_wait_for_response(command_buffer, "+QMTCONN: 1,0,0", 20000)) {
                    is_secondary_connected = true;
                    his_secondary_connected = true;
                    log_message("--- MQTT Secondary Connection Successful ---\r\n");
                } else { log_message("--- MQTT Secondary Connection Failed. ---\r\n"); }
            } else { log_message("--- MQTT Secondary Open Failed. ---\r\n"); }
        }
        #endif




        master_log_payload_t data_to_send;

        if (xQueueReceive(g_data_snapshot_queue, &data_to_send, portMAX_DELAY) == pdPASS)
        {
            char ts[22];


            snprintf(ts, sizeof(ts), "%04d-%02d-%02d %02d:%02d:%02d",
                     2000 + data_to_send.rtc_time.year,
                     data_to_send.rtc_time.month,
                     data_to_send.rtc_time.day_of_month,
                     data_to_send.rtc_time.hour,
                     data_to_send.rtc_time.minutes,
                     data_to_send.rtc_time.seconds);


            if (data_to_send.gps_snapshot.is_valid) {

                char lat_str[25];
                char lon_str[25];
                char speed_str[6];
                char course_str[6];
                char data_validity_str[10];
                char msld_speed_kmh_str[10];


                format_coord(lat_str, sizeof(lat_str), data_to_send.gps_snapshot.latitude, 'N', 'S');
                format_coord(lon_str, sizeof(lon_str), data_to_send.gps_snapshot.longitude, 'E', 'W');
                snprintf(course_str, sizeof(course_str), "%03d", (int)roundf(data_to_send.gps_snapshot.course_degrees));


                float raw_gps_speed_kmh = data_to_send.gps_snapshot.speed_knots * 1.852;

                snprintf(speed_str, sizeof(speed_str), "%02d", (strcmp(data_to_send.vehicle_status, "IDLE") == 0) ? 0 : (int)roundf(raw_gps_speed_kmh));

                strncpy(data_validity_str, "Valid", sizeof(data_validity_str));


                snprintf(msld_speed_kmh_str, sizeof(msld_speed_kmh_str), "%.1f", data_to_send.msld_speed);
                convert_sequence(seq_number_long, seq_number,999);
                seq_number_long++;

                snprintf(mqtt_payload, sizeof(mqtt_payload),
					 "{\"deviceID\":\"%s\",\"IMEI\":\"%s\",\"timestamp\":\"%s\","
					 "\"dataValidity\":\"%s\",\"status\":\"N1\",\"latitude\":\"%s\","
					 "\"longitude\":\"%s\",\"speed\":\"%s\",\"course\":\"%s\","
					 "\"ignition\":\"%s\",\"vehicleStatus\":\"%s\","
					 "\"additionalData\":\"0000000000000\",\"timeIntervals\":\"002,010,002\","
					 "\"angleInterval\":\"015\",\"distanceInterval\":\"100\","
					 "\"gsmStrength\":\"073\",\"sequenceNumber\":\"%s\","
					 "\"msldSpeed_kmh\":%s,"
                     "\"powerStatus\":\"%s\","
                     "\"signalStatus\":\"%s\"}",
					 g_device_id, g_device_imei, ts,
					 data_validity_str, lat_str, lon_str, speed_str, course_str,
					 ignitionStatus, data_to_send.vehicle_status,
					 seq_number,
					 msld_speed_kmh_str,
                     data_to_send.power_status,
                     data_to_send.signal_status);
            } else {

                snprintf(mqtt_payload, sizeof(mqtt_payload),
                    "{\"deviceID\":\"%s\",\"IMEI\":\"%s\",\"timestamp\":\"%s\","
                    "\"dataValidity\":\"Invalid\",\"latitude\":\"SL\",\"longitude\":\"SL\","
                    "\"speed\":\"000\",\"ignition\":\"%s\",\"msldSpeed_kmh\":%.1f,"
                    "\"powerStatus\":\"%s\",\"signalStatus\":\"%s\"}",
                    g_device_id, g_device_imei, ts,
                    ignitionStatus, data_to_send.msld_speed,
                    data_to_send.power_status,
                    data_to_send.signal_status
                );

            }


            if (is_primary_connected) {
                snprintf(command_buffer, sizeof(command_buffer), "AT+QMTPUBEX=0,0,0,0,\"%s\",%d\r\n", g_mqtt_topic, (int)strlen(mqtt_payload));
                if (send_and_wait_for_response(command_buffer, ">", 5000)) {
                    if (send_and_wait_for_response(mqtt_payload, "+QMTPUBEX: 0,0,0", 10000))
                    {
                        log_message("Publish OK (Primary)\r\n");
                        g_successful_publishes++;
                        his_primary_connected=true;

    					for(int loop_CNT=0;loop_CNT<2;loop_CNT++)
    					{
    						backup_UPLOAD = get_aLOG();
    						if(backup_UPLOAD==1)
    						{
    							packetize_history_data();
    							int error_CHECK = publish_history_data(formatting_his_payload);
    							if(error_CHECK==true)
    							{
    								  to_COUNT=to_COUNT-1;
    								  if(to_COUNT<1)
    								  {
    									  to_COUNT=MAX_LOG_COUNT;
    								  }
									  to_COUNTH      = (uint8_t)(to_COUNT >> 8)&0x00FF;
									  to_COUNTL      = (uint8_t)(to_COUNT & 0x00FF);
									  eeprom_WRITEBYTE(TO_ADDR_H,to_COUNTH);
									  eeprom_WRITEBYTE(TO_ADDR_L,to_COUNTL);

    							}
    							else
    							{
    								break;
    							}
    						}
    						else
    						{
    							break;
    						}
    					}
                    } else {
                        is_primary_connected = false;
                        log_message("!!! Publish FAILED (Primary) !!!\r\n");
                        store_history=9;
                    }
                } else {
                    is_primary_connected = false;
                    log_message("!!! Publish FAILED (Primary) !!!\r\n");
                    store_history=9;
                }
            }
            else
            {
            	store_history=9;
            }



            #if USE_SECONDARY_SERVER
            if (is_secondary_connected) {
                snprintf(command_buffer, sizeof(command_buffer), "AT+QMTPUBEX=1,0,0,0,\"%s\",%d\r\n", g_secondary_mqtt_topic, (int)strlen(mqtt_payload));
                if (send_and_wait_for_response(command_buffer, ">", 5000)) {
                    if (send_and_wait_for_response(mqtt_payload, "+QMTPUBEX: 1,0,0", 10000)) {
                        log_message("Publish OK (Secondary)\r\n");
                    } else {
                        is_secondary_connected = false;
                        his_secondary_connected=false;
                        log_message("!!! Publish FAILED (Secondary) !!!\r\n");
                    }
                } else {
                    is_secondary_connected = false;
                    his_secondary_connected=false;
                    log_message("!!! Publish FAILED (Secondary) !!!\r\n");
                }
            }
            #endif


        }
        vTaskDelay(pdMS_TO_TICKS(100));
    }
}

uint8_t publish_history_data(char *history_data_payload_)
{
	char command_buffer[256];
	uint8_t return_status_=0;
    if (his_primary_connected==true)
    {
        snprintf(command_buffer, sizeof(command_buffer), "AT+QMTPUBEX=0,0,0,0,\"%s\",%d\r\n", g_mqtt_topic, (int)strlen(history_data_payload_));
        if (send_and_wait_for_response(command_buffer, ">", 5000)) {
            if (send_and_wait_for_response(history_data_payload_, "+QMTPUBEX: 0,0,0", 10000))
            {
            	return_status_ =  true;

            } else {
                return_status_ =  false;
            }
        } else {
            return_status_ =  false;
        }
    }

	#if USE_SECONDARY_SERVER
	if (his_secondary_connected==true) {
		snprintf(command_buffer, sizeof(command_buffer), "AT+QMTPUBEX=1,0,0,0,\"%s\",%d\r\n", g_secondary_mqtt_topic, (int)strlen(history_data_payload_));
		if (send_and_wait_for_response(command_buffer, ">", 5000)) {
			if (send_and_wait_for_response(history_data_payload_, "+QMTPUBEX: 1,0,0", 10000)) {
				log_message("Publish OK (Secondary)\r\n");
			} else {
				log_message("!!! Publish FAILED (Secondary) !!!\r\n");
			}
		} else {
			log_message("!!! Publish FAILED (Secondary) !!!\r\n");
		}
	}
	#endif
	return return_status_;
}


void flush_modem_uart_buffer(void)
{
    uint8_t flush_char;
    uint16_t flush_count = 0;
    char log_buf[60];

    while (HAL_UART_Receive(&huart1, &flush_char, 1, 10) == HAL_OK && flush_count < 100) {
        flush_count++;
    }

    if (flush_count > 0) {
        snprintf(log_buf, sizeof(log_buf), "Flushed %d bytes from modem buffer\r\n", flush_count);
        log_message(log_buf);
    }
}


bool send_sms(const char* recipient, const char* message)
{
    char cmd_buf[200];
    char ctrl_z = 26;
    char log_buf[80];

    snprintf(log_buf, sizeof(log_buf), "--- SMS to %s ---\r\n", recipient);
    log_message(log_buf);


    if (!send_and_wait_for_response("AT+CMGF=1\r\n", "OK", 3000)) return false;

    snprintf(cmd_buf, sizeof(cmd_buf), "AT+CMGS=\"%s\"\r\n", recipient);
    if (!send_and_wait_for_response(cmd_buf, ">", 8000)) return false;

    vTaskDelay(pdMS_TO_TICKS(200));

    snprintf(cmd_buf, sizeof(cmd_buf), "%s%c", message, ctrl_z);
    if (!send_and_wait_for_response(cmd_buf, "OK", 15000)) return false;

    log_message("SMS sent successfully.\r\n");
    return true;
}

char sms_buffer[512];
char command_buffer[100];
char temp_log[200];

void vSmsTask(void *pvParameters)
{
    vTaskDelay(pdMS_TO_TICKS(15000));


    load_persistent_settings();


    log_message("=Clearing all SMS messages=\r\n");
    send_and_wait_for_response("AT+CMGF=1\r\n", "OK", 3000);
    send_and_wait_for_response("AT+CMGDA=\"DEL ALL\"\r\n", "OK", 5000);
    log_message("messages cleared\r\n");


    while(1)
    {

        vTaskDelay(pdMS_TO_TICKS(3000));

        log_message("-Check SMS Commands-\r\n");

        if (!send_and_wait_for_response("AT+CMGF=1\r\n", "OK", 3000))
        {
            continue;
        }


        if (xSemaphoreTake(g_modem_mutex, pdMS_TO_TICKS(15000)) != pdTRUE) {
             log_message("SMS Task Mutex\r\n");
             continue;
        }

        memset(sms_buffer, 0, sizeof(sms_buffer));
        xQueueReset(g_modem_uart_rx_queue);
        HAL_UART_Transmit(&huart1, (uint8_t*)"AT+CMGL=\"ALL\"\r\n", strlen("AT+CMGL=\"ALL\"\r\n"), 1000);

        uint16_t rx_index = 0;
        TickType_t start_tick = xTaskGetTickCount();

        while((xTaskGetTickCount() - start_tick) < pdMS_TO_TICKS(8000))
        {
            uint8_t queue_rx_buffer[MODEM_UART_RX_BUFFER_SIZE + 1];
            if (xQueueReceive(g_modem_uart_rx_queue, &queue_rx_buffer, pdMS_TO_TICKS(200)) == pdPASS)
            {
                int new_len = strlen((char*)queue_rx_buffer);
                if (rx_index + new_len < sizeof(sms_buffer) - 1)
                {
                    strcat(sms_buffer, (char*)queue_rx_buffer);
                    rx_index += new_len;
                }
                if (strstr(sms_buffer, "\r\nOK\r\n") != NULL || strstr(sms_buffer, "\r\nERROR\r\n") != NULL)
                {
                    break;
                }
            }
        }
        snprintf(temp_log, sizeof(temp_log), "SMS RSP << %s\r\n", sms_buffer);
        log_message(temp_log);
        xSemaphoreGive(g_modem_mutex);



        char* current_msg_ptr = strstr(sms_buffer, "+CMGL:");
        while (current_msg_ptr != NULL)
        {
            int msg_index;
            char sender_number[20] = {0};
            char msg_content[161] = {0};
            bool is_rebooting = false;

            sscanf(current_msg_ptr, "+CMGL: %d,\"%*[^\"]\",\"%[^\"]\"", &msg_index, sender_number);
            snprintf(temp_log, sizeof(temp_log), "Found SMS at index %d from %s\n", msg_index, sender_number);
            log_message(temp_log);

            char* content_start = strchr(current_msg_ptr, '\n');
            if (content_start != NULL)
            {
                content_start++;
                char* content_end = strchr(content_start, '\n');
                if (content_end != NULL)
                {
                    size_t content_len = content_end - content_start;
                    if (content_len < sizeof(msg_content))
                    {
                        strncpy(msg_content, content_start, content_len);
                        if (content_len > 0 && msg_content[content_len - 1] == '\r')
                        {
                            msg_content[content_len - 1] = '\0';
                        }
                    }
                }
            }


            if (strlen(sender_number) > 0)
            {
                char ack_message[160] = "ACK: Unknown Command";
                bool send_ack = false;


                char* value_ptr = strchr(msg_content, ':');
                if (value_ptr != NULL)
                {
                    *value_ptr = '\0';
                    value_ptr++;
                }


                if (strcmp(msg_content, "AUTH") == 0)
                {
                    snprintf(temp_log, sizeof(temp_log), "AUTH command from: %s\r\n", sender_number);
                    log_message(temp_log);

                    strncpy(g_authorized_sender, sender_number, sizeof(g_authorized_sender) - 1);
                    g_authorized_sender[sizeof(g_authorized_sender) - 1] = '\0';

                    eeprom_write_string(AUTH_SENDER_EEPROM_ADDR, g_authorized_sender, AUTH_SENDER_MAX_LEN);

                    snprintf(ack_message, sizeof(ack_message), "ACK: You are now the authorized sender.");
                    send_ack = true;
                }

                else if (strcmp(sender_number, g_authorized_sender) == 0)
                {
                    snprintf(temp_log, sizeof(temp_log), "Authorized message: '%s'\r\n", msg_content);
                    log_message(temp_log);
                    send_ack = true;


                    if (strcmp(msg_content, "UNAUTH") == 0)
                    {
                        log_message("Clearing authorization.\r\n");
                        snprintf(ack_message, sizeof(ack_message), "ACK: Authorization cleared.");
                        strcpy(g_authorized_sender, "UNAUTHORIZED"); // Set to a non-number
                        eeprom_write_string(AUTH_SENDER_EEPROM_ADDR, g_authorized_sender, AUTH_SENDER_MAX_LEN);
                    }
                    else if (strcmp(msg_content, "SET_MQTT_BROKER") == 0 && value_ptr != NULL) {
                        char port[6];
                        sscanf(value_ptr, "%[^,],%s", g_mqtt_broker_ip, port);
                        strcpy(g_mqtt_broker_port, port);
                        snprintf(ack_message, sizeof(ack_message), "ACK: Broker set to %s:%s", g_mqtt_broker_ip, g_mqtt_broker_port);

                        eeprom_write_string(MQTT_BROKER_IP_ADDR, g_mqtt_broker_ip, sizeof(g_mqtt_broker_ip));
                        eeprom_write_string(MQTT_BROKER_PORT_ADDR, g_mqtt_broker_port, sizeof(g_mqtt_broker_port));
                    }
                    else if (strcmp(msg_content, "SET_MQTT_CLIENT") == 0 && value_ptr != NULL) {
                        strncpy(g_mqtt_client_id, value_ptr, sizeof(g_mqtt_client_id) - 1);
                        snprintf(ack_message, sizeof(ack_message), "ACK: Client ID set");
                        eeprom_write_string(MQTT_CLIENT_ID_ADDR, g_mqtt_client_id, sizeof(g_mqtt_client_id));
                    }
                    else if (strcmp(msg_content, "SET_MQTT_USER") == 0 && value_ptr != NULL) {
                        strncpy(g_mqtt_username, value_ptr, sizeof(g_mqtt_username) - 1);
                        snprintf(ack_message, sizeof(ack_message), "ACK: User set");
                        eeprom_write_string(MQTT_USERNAME_ADDR, g_mqtt_username, sizeof(g_mqtt_username));
                    }
                    else if (strcmp(msg_content, "SET_MQTT_PASS") == 0 && value_ptr != NULL) {
                        strncpy(g_mqtt_password, value_ptr, sizeof(g_mqtt_password) - 1);
                        snprintf(ack_message, sizeof(ack_message), "ACK: Password set");
                        eeprom_write_string(MQTT_PASSWORD_ADDR, g_mqtt_password, sizeof(g_mqtt_password));
                    }
                    else if (strcmp(msg_content, "SET_MQTT_TOPIC") == 0 && value_ptr != NULL) {
                        strncpy(g_mqtt_topic, value_ptr, sizeof(g_mqtt_topic) - 1);
                        snprintf(ack_message, sizeof(ack_message), "ACK: Topic set");
                        eeprom_write_string(MQTT_TOPIC_ADDR, g_mqtt_topic, sizeof(g_mqtt_topic));
                    }


                    else if (strcmp(msg_content, "SET_MQTT_BROKER2") == 0 && value_ptr != NULL) {
                        char port[6];
                        sscanf(value_ptr, "%[^,],%s", g_secondary_mqtt_broker_ip, port);
                        strcpy(g_secondary_mqtt_broker_port, port);
                        snprintf(ack_message, sizeof(ack_message), "ACK: Broker 2 set to %s:%s", g_secondary_mqtt_broker_ip, g_secondary_mqtt_broker_port);

                        eeprom_write_string(SEC_MQTT_BROKER_IP_ADDR, g_secondary_mqtt_broker_ip, sizeof(g_secondary_mqtt_broker_ip));
                        eeprom_write_string(SEC_MQTT_BROKER_PORT_ADDR, g_secondary_mqtt_broker_port, sizeof(g_secondary_mqtt_broker_port));
                    }
                    else if (strcmp(msg_content, "SET_MQTT_CLIENT2") == 0 && value_ptr != NULL) {
                        strncpy(g_secondary_mqtt_client_id, value_ptr, sizeof(g_secondary_mqtt_client_id) - 1);
                        snprintf(ack_message, sizeof(ack_message), "ACK: Client ID 2 set");
                        eeprom_write_string(SEC_MQTT_CLIENT_ID_ADDR, g_secondary_mqtt_client_id, sizeof(g_secondary_mqtt_client_id));
                    }
                    else if (strcmp(msg_content, "SET_MQTT_USER2") == 0 && value_ptr != NULL) {
                        strncpy(g_secondary_mqtt_username, value_ptr, sizeof(g_secondary_mqtt_username) - 1);
                        snprintf(ack_message, sizeof(ack_message), "ACK: User 2 set");
                        eeprom_write_string(SEC_MQTT_USERNAME_ADDR, g_secondary_mqtt_username, sizeof(g_secondary_mqtt_username));
                    }
                    else if (strcmp(msg_content, "SET_MQTT_PASS2") == 0 && value_ptr != NULL) {
                        strncpy(g_secondary_mqtt_password, value_ptr, sizeof(g_secondary_mqtt_password) - 1);
                        snprintf(ack_message, sizeof(ack_message), "ACK: Password 2 set");
                        eeprom_write_string(SEC_MQTT_PASSWORD_ADDR, g_secondary_mqtt_password, sizeof(g_secondary_mqtt_password));
                    }
                    else if (strcmp(msg_content, "SET_MQTT_TOPIC2") == 0 && value_ptr != NULL) {
                        strncpy(g_secondary_mqtt_topic, value_ptr, sizeof(g_secondary_mqtt_topic) - 1);
                        snprintf(ack_message, sizeof(ack_message), "ACK: Topic 2 set");
                        eeprom_write_string(SEC_MQTT_TOPIC_ADDR, g_secondary_mqtt_topic, sizeof(g_secondary_mqtt_topic));
                    }


                    else if (strcmp(msg_content, "SET_DEVICE_ID") == 0 && value_ptr != NULL) {
                        strncpy(g_device_id, value_ptr, sizeof(g_device_id) - 1);
                        g_device_id[sizeof(g_device_id) - 1] = '\0';
                        snprintf(ack_message, sizeof(ack_message), "ACK: Device ID set to %s", g_device_id);
                        eeprom_write_string(DEVICE_ID_ADDR, g_device_id, sizeof(g_device_id));
                    }


                    else if (strcmp(msg_content, "SET_TIMEZONE") == 0 && value_ptr != NULL) {

                        if (strlen(value_ptr) == 6 && (value_ptr[0] == '+' || value_ptr[0] == '-') && value_ptr[3] == ':') {
                            strncpy(g_time_zone_str, value_ptr, sizeof(g_time_zone_str) - 1);
                            g_time_zone_str[sizeof(g_time_zone_str) - 1] = '\0';

                            snprintf(ack_message, sizeof(ack_message), "ACK: Timezone set to %s. Will sync on next GPS fix.", g_time_zone_str);
                            eeprom_write_string(TIMEZONE_EEPROM_ADDR, g_time_zone_str, sizeof(g_time_zone_str));


                            g_update_rtc_from_gps_flag = true;
                        } else {
                            snprintf(ack_message, sizeof(ack_message), "ACK: Invalid TZ format. Use +HH:MM or -HH:MM");
                        }
                    }



                    else if (strcmp(msg_content, "GPS_STATUS") == 0) {
                        gps_data_t current_gps_data = {0};
                        if (xSemaphoreTake(g_gps_data_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
                            current_gps_data = g_gps_data;
                            xSemaphoreGive(g_gps_data_mutex);
                        }
                        if (current_gps_data.is_valid) {
                            snprintf(ack_message, sizeof(ack_message), "GPS: %.5f,%.5f Alt:%.1fm Spd:%.1fkn Msgs:%lu",
                                     current_gps_data.latitude, current_gps_data.longitude,
                                     current_gps_data.altitude, current_gps_data.speed_knots, g_successful_publishes);
                        } else {
                            snprintf(ack_message, sizeof(ack_message), "GPS: No Fix. Msgs:%lu Runtime:%lus",
                                     g_successful_publishes, (unsigned long)((HAL_GetTick() - g_session_start_time) / 1000));
                        }
                    }
                    else if (strcmp(msg_content, "STATUS") == 0) {
                        snprintf(ack_message, sizeof(ack_message), "Runtime:%lus Msgs:%lu SpdLim:%u",
                                 (unsigned long)((HAL_GetTick() - g_session_start_time) / 1000), g_successful_publishes, set_speed_eep);
                    }
                    else if (strcmp(msg_content, "SET_SPEED_LIMIT") == 0 && value_ptr != NULL) {
                        uint8_t new_speed = (uint8_t)atoi(value_ptr);
                        if (new_speed >= 5 && new_speed <= 150) {
                            set_speed_eep = new_speed;
                            if (set_speed_flag == 0) {
                                set_speed = set_speed_eep;
                            }
                            snprintf(ack_message, sizeof(ack_message), "ACK: Speed limit set to %u", set_speed_eep);
                        } else {
                            snprintf(ack_message, sizeof(ack_message), "ACK: Invalid speed value (5-150)");
                        }
                    }
                    else if (strcmp(msg_content, "REBOOT") == 0)
                    {
                        snprintf(command_buffer, sizeof(command_buffer), "AT+CMGD=%d\r\n", msg_index);
                        send_and_wait_for_response(command_buffer, "OK", 3000);
                        vTaskDelay(pdMS_TO_TICKS(500));

                        snprintf(ack_message, sizeof(ack_message), "ACK: Rebooting after %lu messages...", g_successful_publishes);
                        send_sms(sender_number, ack_message);

                        vTaskDelay(pdMS_TO_TICKS(2000));

                        log_message("REBOOT System\r\n");
                        is_rebooting = true;
                        HAL_NVIC_SystemReset();
                    }


                }

                else
                {
                    snprintf(temp_log, sizeof(temp_log), "Unauthorized sender: %s\r\n", sender_number);
                    log_message(temp_log);
                    send_ack = false;
                }

                if (send_ack) {
                    send_sms(sender_number, ack_message);
                }
            }

            if (!is_rebooting) {
                snprintf(command_buffer, sizeof(command_buffer), "AT+CMGD=%d\r\n", msg_index);
                send_and_wait_for_response(command_buffer, "OK", 3000);
            }

            current_msg_ptr = strstr(current_msg_ptr + 1, "+CMGL:");
        }
    }
}



void HAL_UARTEx_RxEventCallback(UART_HandleTypeDef *huart, uint16_t Size)
{

   if (huart->Instance == UART12) {
        write_pos = Size;
        BaseType_t xHigherPriorityTaskWoken = pdFALSE;
        xSemaphoreGiveFromISR(g_gps_data_ready_sem, &xHigherPriorityTaskWoken);
        portYIELD_FROM_ISR(xHigherPriorityTaskWoken);
   }

   else if (huart->Instance == USART1) {
        BaseType_t xHigherPriorityTaskWoken = pdFALSE;
        uint8_t received_data[MODEM_UART_RX_BUFFER_SIZE + 1] = {0};
        memcpy(received_data, g_modem_dma_rx_buffer, Size);

        xQueueSendFromISR(g_modem_uart_rx_queue, &received_data, &xHigherPriorityTaskWoken);
        HAL_UARTEx_ReceiveToIdle_DMA(&huart1, g_modem_dma_rx_buffer, MODEM_UART_RX_BUFFER_SIZE);
        portYIELD_FROM_ISR(xHigherPriorityTaskWoken);
   }
   else if(huart->Instance == USART2)
	{

		uart2_rx_size = Size;
		BaseType_t xHigherPriorityTaskWoken = pdFALSE;

	}
}

void HAL_UART_ErrorCallback(UART_HandleTypeDef *huart)
{
    char error_msg[80];
    if (huart->Instance == UART12) {
        if (HAL_UART_GetError(huart) & HAL_UART_ERROR_ORE) {
           snprintf(error_msg, sizeof(error_msg), "!! UART12 (GPS) Overrun Error. Restarting DMA. !!\r\n");
           log_message(error_msg);
            HAL_UART_AbortReceive(huart);
            HAL_UARTEx_ReceiveToIdle_DMA(&huart12, g_gps_dma_rx_buffer, GPS_DMA_RX_BUFFER_SIZE);
        }
    } else if (huart->Instance == USART1) {
         uint32_t error_code = HAL_UART_GetError(huart);
         snprintf(error_msg, sizeof(error_msg), "!! UART1 (Modem) Error, code: 0x%lX. Restarting DMA. !!\r\n", error_code);
         log_message(error_msg);
         HAL_UART_AbortReceive(huart);
         HAL_UARTEx_ReceiveToIdle_DMA(&huart1, g_modem_dma_rx_buffer, MODEM_UART_RX_BUFFER_SIZE);
    }
}

/*
 * MSLD CODE and TASK
 * Variables on the top and in global_declaration.s
 */
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

int _write(int file, char *ptr, int len)
{
	int DataIdx;
	for (DataIdx = 0; DataIdx < len; DataIdx++)
	{

		ITM_SendChar(*ptr++);
	}
	return len;
}

float getSpeed()
{
	float filtered_speed=0.0;
	int test_var[15];
	float frequency_rate=0.0;
	int input_switch_fz_lock=0;

	float alpha = 0.3;

	ppr_value=program_setting_data[pgmr_pointer][6];
	//ppr_value=program_setting_data[dp][6];
	//speed=ppr_value;//
	speed=ppr_value*frequency;//ppr*freq

	//speed=ppr_value*freq;//ppr*freq
	//printf("ppr=%1.3f\n",ppr_value);
	//printf("speed=%2.2f\n",speed);

	/*
	sprintf(test_var,"Speed=%3.3f\n",speed);
	HAL_UART_Transmit(&huart1,(uint8_t*)test_var,(uint16_t)(strlen((char*)test_var)), 1000);
	vTaskDelay(1/portTICK_PERIOD_MS);
	*/

	if((speed<(spd+8)) && (speed>(spd-8)))//if((speed<(spd+8)) && (speed>(spd-8)) )
	{
		speed=speed;
		spd=speed;
		spd_exit=0;
	}
	else
	{
		spd_exit++;
		if(spd_exit>50)
		{
			spd_exit=0;
			spd=speed;
		}
		if(frequency<=0)
		{
			speed=0;
			spd=speed;
		}
		else
		{
			speed=spd;
		}
	}
	return speed;
	//filtered_speed = alpha * speed + (1 - alpha) * previous_speed;
	//previous_speed = speed;
	//return speed;
	//return filtered_speed;
}

float pid_controller(float Kp,float Ki,float Kd,float set_speed,float current_speed,float pedal_voltage_at_rated_speed)
{
	float increase_pedal_percentage=0.0;
	//!Kp-proportional gain
	//!Ki-integral gain
	//!Kd-defferential gain
	//!set_speed-Rated speed
	//!Current_speed-Running speed
	//!pedal_voltage_at_rated_speed-Pedal value at which speed cross the set speed
	//!P=proportional variable
	//!I=Integral variable
	//!D=differential variable
	//!pedal_volatge=Variable to store value of pedal at limiting area.
	//!//
	float P=0;
	float D=0;
	float pedal_voltage=0;
	float temp_ki=0.0;
	float temp_kp=0.0;
	//Get error value form the set speed and current running speed.
	//Simply take difference between set speed and current speed of the vehicle
	error_value=set_speed-current_speed;
	//store the current error value in proportional variable P.
	P=error_value;
	//Get the integral term by simply accumulating the errors generated by the PID controller

	temp_ki = Ki;
	temp_kp = Kp;

	if(I_Clamping==0)
	{
		I_=I_+error_value;
	}

	//Differential varable D can be find out by simply taking the difference between current and previous errors
	D=error_value-pre_error_value;
	//Store the current error values to the previous error variable for getting D term

	//To get the pid controller final action take the product sum of all three terms
	//with the corresponding gain values.And by adding the pedal value at the set speed and pid factor ,
	//to get the pedal voltage to maintain the speed.

	////Edited by Renjith
	/*
	if(pgmr_pointer==0)
	{
		I_ += error_value * 0.01;
		D 	= (error_value-pre_error_value)/0.01;
	}
	*/

	error_percentage = (float)(error_value * 0.833);//Percentage 100 / max speed = 120Km/Hr

	///////////Below line for izusu
	//temp_ki = Ki;
	//error_percentage = (float)(error_value * 0.833);

	if(error_percentage>0.9)//early 0.5 and +0.004
	{
		//I_=I_-(error_value/2);
		//temp_ki = ki;
		temp_ki = Ki+0.003;
	}
	///earlier multiple of 4 now multiple of .35
	increase_pedal_percentage = pedal_voltage_at_rated_speed;
	if(set_speed>=120)
	{
		increase_pedal_percentage = pedal_voltage_at_rated_speed-1.2;//1.34 was better//1.6 not that much//1.2not tested
	}
	else if(set_speed>=110)
	{
		increase_pedal_percentage = pedal_voltage_at_rated_speed-1.2;
	}
	else if(set_speed>=100)
	{
		increase_pedal_percentage = pedal_voltage_at_rated_speed-0.8;
	}
	else if(set_speed>=90)
	{
		increase_pedal_percentage = pedal_voltage_at_rated_speed-0.4;
	}
	else
	{
		increase_pedal_percentage = pedal_voltage_at_rated_speed;
	}

	pid_result = ((temp_kp*P)+(temp_ki*I_)+(Kd*D));
	pedal_voltage=pedal_voltage_at_rated_speed + pid_result;

	if(pedal_voltage<=program_setting_data[pgmr_pointer][0])
	{
		pedal_voltage=program_setting_data[pgmr_pointer][0];
		I_Clamping=1;
	}
	else if(pedal_voltage>=program_setting_data[pgmr_pointer][1])
	{
		pedal_voltage=program_setting_data[pgmr_pointer][1];
		I_Clamping=1;
	}
	else
	{
		I_Clamping=0;
	}
	pre_error_value=error_value;
	return(pedal_voltage);
}
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////










void USB_ForceReenumeration(void)
{
    GPIO_InitTypeDef GPIO_InitStruct = {0};


    __HAL_RCC_USB_CLK_DISABLE();


    __HAL_RCC_GPIOA_CLK_ENABLE();
    GPIO_InitStruct.Pin = GPIO_PIN_12;
    GPIO_InitStruct.Mode = GPIO_MODE_OUTPUT_PP;
    GPIO_InitStruct.Pull = GPIO_NOPULL;
    GPIO_InitStruct.Speed = GPIO_SPEED_FREQ_LOW;
    HAL_GPIO_Init(GPIOA, &GPIO_InitStruct);
    HAL_GPIO_WritePin(GPIOA, GPIO_PIN_12, GPIO_PIN_RESET);


    vTaskDelay(pdMS_TO_TICKS(25));


    HAL_GPIO_DeInit(GPIOA, GPIO_PIN_12);
    __HAL_RCC_USB_CLK_ENABLE();


    vTaskDelay(pdMS_TO_TICKS(25));
}











extern uint8_t UserTxBuffer[64];
extern uint8_t UserRxBuffer[64];
//extern uint8_t USB_Receive_Buffer[];
//extern volatile uint32_t USB_Receive_Index;
extern volatile uint8_t USB_DataReady;
extern volatile uint32_t USB_Receive_Length;
void usb_Task(void *pvParameters);
char temp_array[300];












void usb_Task(void *pvParameters)
{


	int device_state=0;
	uint16_t loop_counter=0;
	uint8_t printer_switch_status=0;
	uint8_t ds3231_buf[7];
	uint8_t rtc_data[7];
	uint8_t buffer[64];
	int print_mode=0;
	int switch_var=0;

	int wait_till_exit=0;

	char parsed_data[45];
	int loop=0;
	int equal_point=0;
	int counter_=0;
	uint8_t printer_counter=0;
	uint8_t print_flag=0;

	for(;;)
	{
		power_input = HAL_GPIO_ReadPin(POWERCUT_IN_GPIO_Port, POWERCUT_IN_Pin);
		if(power_input==1)
		{
			power_limp=9;
			sprintf(power_status,"PowD");
			his_power_cut=0;
		}
		else
		{
			his_power_cut=1;
			power_limp=0;
			sprintf(power_status,"PowC");
		}
		ignition_input = HAL_GPIO_ReadPin(IGNITION_IN_GPIO_Port, IGNITION_IN_Pin);
		if(ignition_input==0)
		{
			sprintf(ignitionStatus,"IGon");
			his_ign_status = 1;
		}
		else
		{
			his_ign_status=0;
			sprintf(ignitionStatus,"IGoff");
			printer_switch_status = HAL_GPIO_ReadPin(SW_IN_2_GPIO_Port, SW_IN_2_Pin);
			if(printer_switch_status==0)
			{
				print_flag=1;
				printer_counter++;
				if(printer_counter>=30)
				{
					print_flag=9;
				}
			}
			else
			{
				if(print_flag!=0)
				{
					printer_counter=0;
					sd_card_busy_printing=9;
					log_message("Printer Data\n");

					if (xSemaphoreTake(g_sd_card_mutex, pdMS_TO_TICKS(2000)) == pdTRUE)
					                    {

					read_header_entry(80);
					device_state = read_circular_voilation_entry(&g_violation_log_file,80,print_flag);
					if(device_state!=99)
					{
						device_state = read_circular_log_entry(&g_circular_log_file,80,print_flag);
					}


					xSemaphoreGive(g_sd_card_mutex);
					                    }
					                    else
					                    {
					                         log_message("SD BUSY: Button Print Skipped\n");
					                    }

					if(device_state==99)
					{
						sprintf(buffer,"\n\n\n\nPrinting Interrupted\n\n\n\n\n");
						HAL_UART_Transmit(&huart2, (uint8_t*)buffer, strlen(buffer), 1000);
					}
					sd_card_busy_printing=0;
					print_flag=0;
					log_message("FINISH Data###\n");
				}
			}


		if(strlen(UserRxBuffer)>0)
		{
			log_message("USB DATA\n");
			counter_++;
			loop_counter=0;
		  if(UserRxBuffer[0]=='*')
		  {

			  header_data_array[0]='\0';
			  wait_till_exit=1;
			  while(wait_till_exit==1)
			  {
				  if(strlen(UserRxBuffer)>0)
				  {
					  loop_counter=0;
					  strcpy(temp_array,UserRxBuffer);
					  strcat(header_data_array,temp_array);

					  memset(temp_array, 0, sizeof(temp_array));
					  memset(UserRxBuffer, 0, sizeof(UserRxBuffer));
					  vTaskDelay(pdMS_TO_TICKS(5));

				  }
				  loop_counter++;
				  if(loop_counter>150)
				  {
					  wait_till_exit=9;

					  vTaskDelay(pdMS_TO_TICKS(25));
				  }
				  vTaskDelay(pdMS_TO_TICKS(10));
			  }
			  if(wait_till_exit==9)
			  {
				  wait_till_exit=0;

				  for(uint16_t k=0;k<290;k++)
				  {
					  temp_array[k]=header_data_array[k+3];
					  if(temp_array[k]=='#')
					  {
						  temp_array[k-1]='\0';
						  log_message("<");
						  log_message(temp_array);
						  log_message(">\n");
						  k=999;

					  }
				  }
				  if (xSemaphoreTake(g_sd_card_mutex, pdMS_TO_TICKS(1000)) == pdTRUE)
				                    {
				  write_header_entry(temp_array);
				  xSemaphoreGive(g_sd_card_mutex);
				                    }
				  vTaskDelay(pdMS_TO_TICKS(5));

			  }
		  }
		  else
		  {
			  strcpy(temp_array,UserRxBuffer);
			  memset(UserRxBuffer, 0, sizeof(UserRxBuffer));
			  for(loop=0;loop<=strlen(temp_array);loop++)
			  {
				  if(temp_array[loop]=='=')
				  {
					  equal_point = loop;
					  loop=99;
				  }
			  }
			  if(temp_array[equal_point]=='=')
			  {
				switch_var =  temp_array[equal_point-1] - 48;
				if(((switch_var>=18)&&(switch_var<=25))||(switch_var==29))
				{
				  print_mode=temp_array[0];
				}
			  }

			  char *ptr = strchr(temp_array, '=');
			  if (ptr != NULL) {
				  strcpy(parsed_data, ptr + 1);
				  parsed_data[strcspn(parsed_data, "\r\n")] = '\0';
			  }


			  sprintf(buffer, "Mode=%u",print_mode);
			  log_message(buffer);

			  vTaskDelay(pdMS_TO_TICKS(5));
			  switch (switch_var)
			  {
				  case 17:
					      if (xSemaphoreTake(g_sd_card_mutex, pdMS_TO_TICKS(1000)) == pdTRUE) {
					      write_header_entry(header_data_array);
					      xSemaphoreGive(g_sd_card_mutex); // END MUTEX
					      log_message("Save Header details\n");
					      }
					      break;
				  case 18:
					      sd_card_busy_printing=9; // Signal to logic, but Mutex does the real work
					      // START MUTEX
					      if (xSemaphoreTake(g_sd_card_mutex, pdMS_TO_TICKS(1000)) == pdTRUE) {
					      read_header_entry(print_mode);
					      memset(UserTxBuffer, 0, sizeof(UserTxBuffer));
					      device_state = read_circular_voilation_entry(&g_violation_log_file,print_mode,8);
					      if(device_state!=99)
					      {
					      read_circular_log_entry(&g_circular_log_file,print_mode,8);
					      }
					      xSemaphoreGive(g_sd_card_mutex); // END MUTEX
					      }
					      log_message("get header & data\n");
					      break;
				  case 19:
					      sd_card_busy_printing=9;
					      if (xSemaphoreTake(g_sd_card_mutex, pdMS_TO_TICKS(1000)) == pdTRUE) {
					      read_header_entry(print_mode);
					      xSemaphoreGive(g_sd_card_mutex);
					      }
					      log_message("get header\n");
					      break;
				  case 20:
					      sd_card_busy_printing=9;
					      if (xSemaphoreTake(g_sd_card_mutex, pdMS_TO_TICKS(1000)) == pdTRUE) {
					      read_circular_log_entry(&g_circular_log_file,print_mode,8);
					      xSemaphoreGive(g_sd_card_mutex);
					      }
					      log_message("get data\n");
					      break;
				  case 21:
					      sd_card_busy_printing=9;
					      vTaskDelay(pdMS_TO_TICKS(100)); // Delay is okay, but lock Mutex only for file op
					      if (xSemaphoreTake(g_sd_card_mutex, pdMS_TO_TICKS(1000)) == pdTRUE) {
					      read_circular_log_entry(&g_circular_log_file,print_mode,1);
					      xSemaphoreGive(g_sd_card_mutex);
					      }
					      log_message("print mode-1\n");
					      break;
				  case 22:
					      sd_card_busy_printing=9;
					      if (xSemaphoreTake(g_sd_card_mutex, pdMS_TO_TICKS(1000)) == pdTRUE) {
					      read_circular_log_entry(&g_circular_log_file,print_mode,6);
					      xSemaphoreGive(g_sd_card_mutex);
					      }
					      log_message("print mode-6\n");
					      break;
				  case 23:
					      sd_card_busy_printing=9;
					      if (xSemaphoreTake(g_sd_card_mutex, pdMS_TO_TICKS(1000)) == pdTRUE) {
					      read_circular_voilation_entry(&g_violation_log_file,print_mode,6);
					      xSemaphoreGive(g_sd_card_mutex);
					      }
					      log_message("print violation\n");
					      break;
				  case 24:

					  log_message("reset\n");
					  break;
				  case 25:

					  sprintf(buffer,"Formatting...\n");
					  log_message(buffer);
					  // Formatting takes time, increase timeout to 5000ms
					  if (xSemaphoreTake(g_sd_card_mutex, pdMS_TO_TICKS(5000)) == pdTRUE) {
					  preallocate_file(DATALOG_FILE, 128);
					  xSemaphoreGive(g_sd_card_mutex);
					  }
					  break;
				  case 26:

					  sprintf(buffer,"set speed=%u\n",device_limp_speed);
					  log_message("limp speed\n");
					  break;
				  case 27:

					  log_message("date & time\n");

					  for(int i=0; i<6; i++) {
						  char high = parsed_data[i*2];
						  char low  = parsed_data[i*2 + 1];
						  rtc_data[i] = ((high - '0') << 4) | (low - '0');
					  }
					  ds3231_buf[0] = rtc_data[5]; // Seconds
					  ds3231_buf[1] = rtc_data[4]; // Minutes
					  ds3231_buf[2] = rtc_data[3]; // Hours
					  ds3231_buf[3] = 0x01;        // Day of week (set Monday for now)
					  ds3231_buf[4] = rtc_data[2]; // Date
					  ds3231_buf[5] = rtc_data[1]; // Month
					  ds3231_buf[6] = rtc_data[0]; // Year

					  HAL_I2C_Mem_Write(&hi2c4, DS3231_I2C_ADDR, DS3231_REG_SECONDS, 1, ds3231_buf, 7, HAL_MAX_DELAY);
					break;
				  case 28:
					  log_message("don't know-2\n");
					  break;
				  case 29:
					  log_message("don't know-3\n");
					  break;
				}
			  sd_card_busy_printing=0;
			  vTaskDelay(pdMS_TO_TICKS(5));
			  UserRxBuffer[0]='\0';

		  }
	  }
		}
	  vTaskDelay(pdMS_TO_TICKS(100));
	}
}



















int main(void)
{





BaseType_t xReturned;




  HAL_Init();
  SystemClock_Config();


  MX_GPIO_Init();
  MX_GPDMA2_Init();
  MX_ADC2_Init();
  MX_I2C1_Init();
  MX_I2C4_Init();
  MX_SPI2_Init();
  MX_SPI3_Init();
  MX_TIM2_Init();
  MX_UART12_Init();
  MX_USART1_UART_Init();
  MX_USART2_UART_Init();
  MX_USART6_UART_Init();
  MX_ICACHE_Init();
  MX_ADC1_Init();
  MX_FATFS_Init();




HAL_GPIO_WritePin(MCP4922_LDAC_GPIO_Port, MCP4922_LDAC_Pin, GPIO_PIN_SET);



	ignition_input = HAL_GPIO_ReadPin(IGNITION_IN_GPIO_Port, IGNITION_IN_Pin);
	if(ignition_input==0)
	{
	sprintf(ignitionStatus,"IGon");
	}
	else
	{
	sprintf(ignitionStatus,"IGoff");
	}







	MX_USB_PCD_Init();
	if(USBD_Init(&hUsbDeviceFS, &Class_Desc, 0) != USBD_OK)
		Error_Handler();



	if(USBD_RegisterClassComposite(&hUsbDeviceFS, USBD_CDC_CLASS, CLASS_TYPE_CDC, CDC_EpAdd_Inst) != USBD_OK)
		Error_Handler();

	if (USBD_CMPSIT_SetClassID(&hUsbDeviceFS, CLASS_TYPE_CDC, 0) != 0xFF)
	{
		USBD_CDC_RegisterInterface(&hUsbDeviceFS, &USBD_CDC_Template_fops);
	}





	HAL_GPIO_WritePin(POWER_LED_GPIO_Port, POWER_LED_Pin, GPIO_PIN_SET);

	log_message("\r\n--- System Initialized. Starting scheduler... ---\r\n");

	HAL_Delay(750);




	handheld_data_ready_sem = xSemaphoreCreateBinary();
	g_gps_data_ready_sem = xSemaphoreCreateBinary();
	g_gps_data_mutex = xSemaphoreCreateMutex();
	g_sd_card_mutex = xSemaphoreCreateMutex();
	g_log_queue = xQueueCreate(LOG_QUEUE_SIZE, sizeof(char[MAX_LOG_MSG_LEN]));
	g_modem_uart_rx_queue = xQueueCreate(MODEM_UART_RX_QUEUE_LEN, MODEM_UART_RX_BUFFER_SIZE + 1);
	g_data_snapshot_queue = xQueueCreate(1, sizeof(master_log_payload_t));
    g_modem_mutex = xSemaphoreCreateMutex();






	xTaskCreate(vStatusLedTask, "StatusLedTask", 512, NULL, 3, NULL);
	xTaskCreate(vLoggingTask, "LoggingTask", 1024, NULL, 5, NULL);
	xTaskCreate(vMqttTask, "MqttTask", 4096, NULL,  6, NULL);
	xTaskCreate(vCircularTask, "CircularLogTask", 4096, NULL, 6, NULL);
	xTaskCreate(vGpsTask, "GpsTask", 3072, NULL, 5, NULL);
	xTaskCreate(vSmsTask, "SmsTask", 3072, NULL, tskIDLE_PRIORITY + 5, NULL);
    xTaskCreate( SYHandTask,"Indication",3072, NULL,6 , NULL );
    xTaskCreate( iHandTask, "pedal_read",512,NULL,4, NULL );
    xTaskCreate( time_jobs, "Handler",512, NULL,tskIDLE_PRIORITY + 5, NULL );
    xTaskCreate( HH_dma,"Indication",1024,NULL, 4, NULL );
    xTaskCreate( vHandTask, "Handler",728, NULL,6, &xTaskHandler2 );
    xTaskCreate( usb_Task,"Indication",4072,NULL, 4, NULL );




	HAL_UARTEx_ReceiveToIdle_DMA(&huart2, handheld_serial_data, DMA_BUFFER_SIZE);
	HAL_TIM_Base_Start(&htim2);

	HAL_NVIC_EnableIRQ(GPDMA2_Channel6_IRQn);
	HAL_NVIC_EnableIRQ(GPDMA2_Channel7_IRQn);
	HAL_NVIC_EnableIRQ(UART12_IRQn);
	HAL_NVIC_EnableIRQ(USART1_IRQn);
	HAL_NVIC_EnableIRQ(EXTI2_IRQn);

	vTaskStartScheduler();


  while (1)
  {

  }

}


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
    Error_Handler();
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
    Error_Handler();
  }


  HAL_RCC_EnableCSS();


  __HAL_FLASH_SET_PROGRAM_DELAY(FLASH_PROGRAMMING_DELAY_2);
}


static void MX_ADC1_Init(void)
{



  ADC_ChannelConfTypeDef sConfig = {0};


  hadc1.Instance = ADC1;
  hadc1.Init.ClockPrescaler = ADC_CLOCK_ASYNC_DIV2;
  hadc1.Init.Resolution = ADC_RESOLUTION_12B;
  hadc1.Init.DataAlign = ADC_DATAALIGN_RIGHT;
  hadc1.Init.ScanConvMode = ADC_SCAN_DISABLE;
  hadc1.Init.EOCSelection = ADC_EOC_SINGLE_CONV;
  hadc1.Init.LowPowerAutoWait = DISABLE;
  hadc1.Init.ContinuousConvMode = DISABLE;
  hadc1.Init.NbrOfConversion = 1;
  hadc1.Init.DiscontinuousConvMode = DISABLE;
  hadc1.Init.ExternalTrigConv = ADC_SOFTWARE_START;
  hadc1.Init.ExternalTrigConvEdge = ADC_EXTERNALTRIGCONVEDGE_NONE;
  hadc1.Init.DMAContinuousRequests = DISABLE;
  hadc1.Init.SamplingMode = ADC_SAMPLING_MODE_NORMAL;
  hadc1.Init.Overrun = ADC_OVR_DATA_PRESERVED;
  hadc1.Init.OversamplingMode = DISABLE;
  if (HAL_ADC_Init(&hadc1) != HAL_OK)
  {
    Error_Handler();
  }


  sConfig.Channel = ADC_CHANNEL_4;
  sConfig.Rank = ADC_REGULAR_RANK_1;
  sConfig.SamplingTime = ADC_SAMPLETIME_2CYCLES_5;
  sConfig.SingleDiff = ADC_SINGLE_ENDED;
  sConfig.OffsetNumber = ADC_OFFSET_NONE;
  sConfig.Offset = 0;
  if (HAL_ADC_ConfigChannel(&hadc1, &sConfig) != HAL_OK)
  {
    Error_Handler();
  }


}


static void MX_ADC2_Init(void)
{



  ADC_ChannelConfTypeDef sConfig = {0};


  hadc2.Instance = ADC2;
  hadc2.Init.ClockPrescaler = ADC_CLOCK_ASYNC_DIV2;
  hadc2.Init.Resolution = ADC_RESOLUTION_12B;
  hadc2.Init.DataAlign = ADC_DATAALIGN_RIGHT;
  hadc2.Init.ScanConvMode = ADC_SCAN_DISABLE;
  hadc2.Init.EOCSelection = ADC_EOC_SINGLE_CONV;
  hadc2.Init.LowPowerAutoWait = DISABLE;
  hadc2.Init.ContinuousConvMode = DISABLE;
  hadc2.Init.NbrOfConversion = 1;
  hadc2.Init.DiscontinuousConvMode = DISABLE;
  hadc2.Init.ExternalTrigConv = ADC_SOFTWARE_START;
  hadc2.Init.ExternalTrigConvEdge = ADC_EXTERNALTRIGCONVEDGE_NONE;
  hadc2.Init.DMAContinuousRequests = DISABLE;
  hadc2.Init.SamplingMode = ADC_SAMPLING_MODE_NORMAL;
  hadc2.Init.Overrun = ADC_OVR_DATA_PRESERVED;
  hadc2.Init.OversamplingMode = DISABLE;
  if (HAL_ADC_Init(&hadc2) != HAL_OK)
  {
    Error_Handler();
  }


  sConfig.Channel = ADC_CHANNEL_8;
  sConfig.Rank = ADC_REGULAR_RANK_1;
  sConfig.SamplingTime = ADC_SAMPLETIME_2CYCLES_5;
  sConfig.SingleDiff = ADC_SINGLE_ENDED;
  sConfig.OffsetNumber = ADC_OFFSET_NONE;
  sConfig.Offset = 0;
  if (HAL_ADC_ConfigChannel(&hadc2, &sConfig) != HAL_OK)
  {
    Error_Handler();
  }


}


static void MX_GPDMA2_Init(void)
{




  __HAL_RCC_GPDMA2_CLK_ENABLE();

  /* GPDMA2 interrupt Init */
    HAL_NVIC_SetPriority(GPDMA2_Channel5_IRQn, 10, 0);
    //HAL_NVIC_EnableIRQ(GPDMA2_Channel5_IRQn);
    HAL_NVIC_SetPriority(GPDMA2_Channel6_IRQn, 10, 0);
   // HAL_NVIC_EnableIRQ(GPDMA2_Channel6_IRQn);
    HAL_NVIC_SetPriority(GPDMA2_Channel7_IRQn, 10, 0);
 //   HAL_NVIC_EnableIRQ(GPDMA2_Channel7_IRQn);



}


static void MX_I2C1_Init(void)
{


  hi2c1.Instance = I2C1;
  hi2c1.Init.Timing = 0x20B0BBFF;
  hi2c1.Init.OwnAddress1 = 0;
  hi2c1.Init.AddressingMode = I2C_ADDRESSINGMODE_7BIT;
  hi2c1.Init.DualAddressMode = I2C_DUALADDRESS_DISABLE;
  hi2c1.Init.OwnAddress2 = 0;
  hi2c1.Init.OwnAddress2Masks = I2C_OA2_NOMASK;
  hi2c1.Init.GeneralCallMode = I2C_GENERALCALL_DISABLE;
  hi2c1.Init.NoStretchMode = I2C_NOSTRETCH_DISABLE;
  if (HAL_I2C_Init(&hi2c1) != HAL_OK)
  {
    Error_Handler();
  }


  if (HAL_I2CEx_ConfigAnalogFilter(&hi2c1, I2C_ANALOGFILTER_ENABLE) != HAL_OK)
  {
    Error_Handler();
  }


  if (HAL_I2CEx_ConfigDigitalFilter(&hi2c1, 0) != HAL_OK)
  {
    Error_Handler();
  }


}


static void MX_I2C4_Init(void)
{


  hi2c4.Instance = I2C4;
  hi2c4.Init.Timing = 0xE020202F;
  hi2c4.Init.OwnAddress1 = 0;
  hi2c4.Init.AddressingMode = I2C_ADDRESSINGMODE_7BIT;
  hi2c4.Init.DualAddressMode = I2C_DUALADDRESS_DISABLE;
  hi2c4.Init.OwnAddress2 = 0;
  hi2c4.Init.OwnAddress2Masks = I2C_OA2_NOMASK;
  hi2c4.Init.GeneralCallMode = I2C_GENERALCALL_DISABLE;
  hi2c4.Init.NoStretchMode = I2C_NOSTRETCH_DISABLE;
  if (HAL_I2C_Init(&hi2c4) != HAL_OK)
  {
    Error_Handler();
  }


  if (HAL_I2CEx_ConfigAnalogFilter(&hi2c4, I2C_ANALOGFILTER_ENABLE) != HAL_OK)
  {
    Error_Handler();
  }


  if (HAL_I2CEx_ConfigDigitalFilter(&hi2c4, 0) != HAL_OK)
  {
    Error_Handler();
  }


}


static void MX_ICACHE_Init(void)
{


  if (HAL_ICACHE_Enable() != HAL_OK)
  {
    Error_Handler();
  }


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
  hspi2.Init.BaudRatePrescaler = SPI_BAUDRATEPRESCALER_64;
  hspi2.Init.FirstBit = SPI_FIRSTBIT_MSB;
  hspi2.Init.TIMode = SPI_TIMODE_DISABLE;
  hspi2.Init.CRCCalculation = SPI_CRCCALCULATION_DISABLE;
  hspi2.Init.CRCPolynomial = 0x7;
  hspi2.Init.NSSPMode = SPI_NSS_PULSE_ENABLE;
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
    Error_Handler();
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
    Error_Handler();
  }


}


static void MX_TIM2_Init(void)
{



  TIM_ClockConfigTypeDef sClockSourceConfig = {0};
  TIM_MasterConfigTypeDef sMasterConfig = {0};


  htim2.Instance = TIM2;
  htim2.Init.Prescaler = 3500;
  htim2.Init.CounterMode = TIM_COUNTERMODE_UP;
  htim2.Init.Period = 131702;
  htim2.Init.ClockDivision = TIM_CLOCKDIVISION_DIV1;
  htim2.Init.AutoReloadPreload = TIM_AUTORELOAD_PRELOAD_DISABLE;
  if (HAL_TIM_Base_Init(&htim2) != HAL_OK)
  {
    Error_Handler();
  }
  sClockSourceConfig.ClockSource = TIM_CLOCKSOURCE_INTERNAL;
  if (HAL_TIM_ConfigClockSource(&htim2, &sClockSourceConfig) != HAL_OK)
  {
    Error_Handler();
  }
  sMasterConfig.MasterOutputTrigger = TIM_TRGO_RESET;
  sMasterConfig.MasterSlaveMode = TIM_MASTERSLAVEMODE_DISABLE;
  if (HAL_TIMEx_MasterConfigSynchronization(&htim2, &sMasterConfig) != HAL_OK)
  {
    Error_Handler();
  }


}


static void MX_UART12_Init(void)
{

  huart12.Instance = UART12;
  huart12.Init.BaudRate = 115200;
  huart12.Init.WordLength = UART_WORDLENGTH_8B;
  huart12.Init.StopBits = UART_STOPBITS_1;
  huart12.Init.Parity = UART_PARITY_NONE;
  huart12.Init.Mode = UART_MODE_TX_RX;
  huart12.Init.HwFlowCtl = UART_HWCONTROL_NONE;
  huart12.Init.OverSampling = UART_OVERSAMPLING_16;
  huart12.Init.OneBitSampling = UART_ONE_BIT_SAMPLE_DISABLE;
  huart12.Init.ClockPrescaler = UART_PRESCALER_DIV1;
  huart12.AdvancedInit.AdvFeatureInit = UART_ADVFEATURE_NO_INIT;
  if (HAL_UART_Init(&huart12) != HAL_OK)
  {
    Error_Handler();
  }
  if (HAL_UARTEx_SetTxFifoThreshold(&huart12, UART_TXFIFO_THRESHOLD_1_8) != HAL_OK)
  {
    Error_Handler();
  }
  if (HAL_UARTEx_SetRxFifoThreshold(&huart12, UART_RXFIFO_THRESHOLD_1_8) != HAL_OK)
  {
    Error_Handler();
  }
  if (HAL_UARTEx_DisableFifoMode(&huart12) != HAL_OK)
  {
    Error_Handler();
  }


}


static void MX_USART1_UART_Init(void)
{


  huart1.Instance = USART1;
  huart1.Init.BaudRate = 115200;
  huart1.Init.WordLength = UART_WORDLENGTH_8B;
  huart1.Init.StopBits = UART_STOPBITS_1;
  huart1.Init.Parity = UART_PARITY_NONE;
  huart1.Init.Mode = UART_MODE_TX_RX;
  huart1.Init.HwFlowCtl = UART_HWCONTROL_NONE;
  huart1.Init.OverSampling = UART_OVERSAMPLING_16;
  huart1.Init.OneBitSampling = UART_ONE_BIT_SAMPLE_DISABLE;
  huart1.Init.ClockPrescaler = UART_PRESCALER_DIV1;
  huart1.AdvancedInit.AdvFeatureInit = UART_ADVFEATURE_NO_INIT;
  if (HAL_UART_Init(&huart1) != HAL_OK)
  {
    Error_Handler();
  }
  if (HAL_UARTEx_SetTxFifoThreshold(&huart1, UART_TXFIFO_THRESHOLD_1_8) != HAL_OK)
  {
    Error_Handler();
  }
  if (HAL_UARTEx_SetRxFifoThreshold(&huart1, UART_RXFIFO_THRESHOLD_1_8) != HAL_OK)
  {
    Error_Handler();
  }
  if (HAL_UARTEx_DisableFifoMode(&huart1) != HAL_OK)
  {
    Error_Handler();
  }


}

static void MX_USART2_UART_Init(void)
{


  huart2.Instance = USART2;
  huart2.Init.BaudRate = 9600;
  huart2.Init.WordLength = UART_WORDLENGTH_8B;
  huart2.Init.StopBits = UART_STOPBITS_1;
  huart2.Init.Parity = UART_PARITY_NONE;
  huart2.Init.Mode = UART_MODE_TX_RX;
  huart2.Init.HwFlowCtl = UART_HWCONTROL_NONE;
  huart2.Init.OverSampling = UART_OVERSAMPLING_16;
  huart2.Init.OneBitSampling = UART_ONE_BIT_SAMPLE_DISABLE;
  huart2.Init.ClockPrescaler = UART_PRESCALER_DIV1;
  huart2.AdvancedInit.AdvFeatureInit = UART_ADVFEATURE_NO_INIT;
  if (HAL_UART_Init(&huart2) != HAL_OK)
  {
    Error_Handler();
  }
  if (HAL_UARTEx_SetTxFifoThreshold(&huart2, UART_TXFIFO_THRESHOLD_1_8) != HAL_OK)
  {
    Error_Handler();
  }
  if (HAL_UARTEx_SetRxFifoThreshold(&huart2, UART_RXFIFO_THRESHOLD_1_8) != HAL_OK)
  {
    Error_Handler();
  }
  if (HAL_UARTEx_DisableFifoMode(&huart2) != HAL_OK)
  {
    Error_Handler();
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
    Error_Handler();
  }
  if (HAL_UARTEx_SetTxFifoThreshold(&huart6, UART_TXFIFO_THRESHOLD_1_8) != HAL_OK)
  {
    Error_Handler();
  }
  if (HAL_UARTEx_SetRxFifoThreshold(&huart6, UART_RXFIFO_THRESHOLD_1_8) != HAL_OK)
  {
    Error_Handler();
  }
  if (HAL_UARTEx_DisableFifoMode(&huart6) != HAL_OK)
  {
    Error_Handler();
  }


}


void MX_USB_PCD_Init(void)
{


  hpcd_USB_DRD_FS.Instance = USB_DRD_FS;
  hpcd_USB_DRD_FS.Init.dev_endpoints = 8;
  hpcd_USB_DRD_FS.Init.speed = USBD_FS_SPEED;
  hpcd_USB_DRD_FS.Init.phy_itface = PCD_PHY_EMBEDDED;
  hpcd_USB_DRD_FS.Init.Sof_enable = DISABLE;
  hpcd_USB_DRD_FS.Init.low_power_enable = DISABLE;
  hpcd_USB_DRD_FS.Init.lpm_enable = DISABLE;
  hpcd_USB_DRD_FS.Init.battery_charging_enable = DISABLE;
  hpcd_USB_DRD_FS.Init.vbus_sensing_enable = DISABLE;
  hpcd_USB_DRD_FS.Init.bulk_doublebuffer_enable = DISABLE;
  hpcd_USB_DRD_FS.Init.iso_singlebuffer_enable = DISABLE;
  if (HAL_PCD_Init(&hpcd_USB_DRD_FS) != HAL_OK)
  {
    Error_Handler();
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


  HAL_GPIO_WritePin(GPIOE, OUTPUT_BUZZER_Pin|VALVE_OUT_Pin, GPIO_PIN_RESET);


  HAL_GPIO_WritePin(GPIOA, GSM_INDICATION_Pin|OPTO_OP_Pin, GPIO_PIN_RESET);


  HAL_GPIO_WritePin(GPIOB, SD_CS_Pin|GPS_RESET_Pin|CTRL_INDICATION_Pin|GSM_RESET_Pin
                          |GSM_POWERKEY_Pin, GPIO_PIN_RESET);


  HAL_GPIO_WritePin(GPIOD, SGL_INDICATION_Pin|GPS_INDICATION_Pin|MCP4922_SS_Pin|MCP4922_LDAC_Pin, GPIO_PIN_RESET);


  HAL_GPIO_WritePin(POWER_LED_GPIO_Port, POWER_LED_Pin, GPIO_PIN_RESET);


  GPIO_InitStruct.Pin = OUTPUT_BUZZER_Pin|VALVE_OUT_Pin;
  GPIO_InitStruct.Mode = GPIO_MODE_OUTPUT_PP;
  GPIO_InitStruct.Pull = GPIO_NOPULL;
  GPIO_InitStruct.Speed = GPIO_SPEED_FREQ_LOW;
  HAL_GPIO_Init(GPIOE, &GPIO_InitStruct);


  GPIO_InitStruct.Pin = SW_IN_2_Pin|SEATBELT_IN_Pin|POWERCUT_IN_Pin|IGNITION_IN_Pin
                          |GSM_STATUS_Pin;
  GPIO_InitStruct.Mode = GPIO_MODE_INPUT;
  GPIO_InitStruct.Pull = GPIO_NOPULL;
  HAL_GPIO_Init(GPIOA, &GPIO_InitStruct);


  GPIO_InitStruct.Pin = SPEED_SGL_P_Pin;
  GPIO_InitStruct.Mode = GPIO_MODE_IT_FALLING;
  GPIO_InitStruct.Pull = GPIO_PULLUP;
  HAL_GPIO_Init(SPEED_SGL_P_GPIO_Port, &GPIO_InitStruct);


  GPIO_InitStruct.Pin = GSM_INDICATION_Pin|OPTO_OP_Pin;
  GPIO_InitStruct.Mode = GPIO_MODE_OUTPUT_PP;
  GPIO_InitStruct.Pull = GPIO_NOPULL;
  GPIO_InitStruct.Speed = GPIO_SPEED_FREQ_LOW;
  HAL_GPIO_Init(GPIOA, &GPIO_InitStruct);


  GPIO_InitStruct.Pin = SD_CS_Pin|GPS_RESET_Pin|CTRL_INDICATION_Pin|GSM_RESET_Pin
                          |GSM_POWERKEY_Pin;
  GPIO_InitStruct.Mode = GPIO_MODE_OUTPUT_PP;
  GPIO_InitStruct.Pull = GPIO_NOPULL;
  GPIO_InitStruct.Speed = GPIO_SPEED_FREQ_LOW;
  HAL_GPIO_Init(GPIOB, &GPIO_InitStruct);


  GPIO_InitStruct.Pin = SGL_INDICATION_Pin|GPS_INDICATION_Pin|MCP4922_SS_Pin|MCP4922_LDAC_Pin;
  GPIO_InitStruct.Mode = GPIO_MODE_OUTPUT_PP;
  GPIO_InitStruct.Pull = GPIO_NOPULL;
  GPIO_InitStruct.Speed = GPIO_SPEED_FREQ_LOW;
  HAL_GPIO_Init(GPIOD, &GPIO_InitStruct);


  GPIO_InitStruct.Pin = POWER_LED_Pin;
  GPIO_InitStruct.Mode = GPIO_MODE_OUTPUT_PP;
  GPIO_InitStruct.Pull = GPIO_NOPULL;
  GPIO_InitStruct.Speed = GPIO_SPEED_FREQ_LOW;
  HAL_GPIO_Init(POWER_LED_GPIO_Port, &GPIO_InitStruct);


  GPIO_InitStruct.Pin = PGMR_CONE_Pin;
  GPIO_InitStruct.Mode = GPIO_MODE_INPUT;
  GPIO_InitStruct.Pull = GPIO_NOPULL;
  HAL_GPIO_Init(PGMR_CONE_GPIO_Port, &GPIO_InitStruct);


  GPIO_InitStruct.Pin = SW_IN_1_Pin;
  GPIO_InitStruct.Mode = GPIO_MODE_INPUT;
  GPIO_InitStruct.Pull = GPIO_NOPULL;
  HAL_GPIO_Init(SW_IN_1_GPIO_Port, &GPIO_InitStruct);


  HAL_NVIC_SetPriority(EXTI2_IRQn, 9, 0);


}





int a1,a2;
int a5=0;
int a=0;
char debug_print_var[100];
uint8_t counter_=0;
uint8_t data_len=0;


int inside_pid_loop_flag=0;



void time_jobs( void *pvParameters )
{
	char testvar[25];
	a1=a2=0;
	counter_=0;
	for(;;)
	{
		dma_hand_held.st_counter++;
		//a5++;
		a1++;
		a2++;
		if(a1>250) a1=0;
		if(a2>250) a2=0;
		if(a1>248)
		{
			a1=0;
			log_message("time_jobs\r\n");
		}
		if(dma_hand_held.st_counter>5)
		{
			dma_hand_held.st_counter=0;
			dma_hand_held.st_SFlg=9;
			if(dma_hand_held.st_SFlg==9)
			{
				counter_++;
				previous_COUNT = current_COUNT;
				dma_hand_held.st_counter=0;
				dma_hand_held.st_SFlg=0;

				current_COUNT = __HAL_DMA_GET_COUNTER(huart2.hdmarx);

				if(current_COUNT!=previous_COUNT)
				{
					handheld_dma_rx=0;

					current_COUNT = __HAL_DMA_GET_COUNTER(huart2.hdmarx);
					data_len = parse_received_dma_data(parse_dma_rx_data);
					sprintf(testvar,"\nDLEN=%u\n",data_len);

					log_message(testvar);
					if(data_len>0)
					{
						sprintf(testvar,"\nParsed=%u,%u\n",counter_,uart2_rx_size);
						log_message(testvar);
						log_message(parse_dma_rx_data);

						sprintf(testvar,"\nrem=%u\n",current_COUNT);

						log_message(testvar);
						handheld_dma_rx=9;

					}
					else
					{
						sprintf(testvar,"\nNO DATA\n");
						log_message(testvar);

					}
				}
			}
		}
		vTaskDelay(pdMS_TO_TICKS(100));
	}
}
uint16_t spi_data=0;
uint8_t gps_limp_time=0;
uint8_t gps_limp_flag=0;
void vHandTask( void *pvParameters )
{
	float temp_frequency=0;
	float frequency_per_speed_=0.0;
	char testarray[20];
	uint32_t ulNotifyValue;

	uint16_t stacktestflag=0;

	vTaskDelay(pdMS_TO_TICKS(400));
	Is_First_Captured=0;


	for(;;)
	{

		ulNotifyValue = ulTaskNotifyTake(pdTRUE,1000);

		if(ulNotifyValue != 0)
		{

			if (Is_First_Captured==0)
			{
				__HAL_TIM_SET_COUNTER(&htim2, 0);

				Is_First_Captured = 1;
			}
			else
			{
				Timer_count = (uint32_t)__HAL_TIM_GET_COUNTER(&htim2);

				if(Timer_count>0)
				{

					temp_frequency = 42000.0f/(float)Timer_count;
					if(temp_frequency<=10000)
					{
						frequency = temp_frequency;
					}










				}

				Is_First_Captured = 0;
				ch_ok=1;
			}

			frequency_flag = 1;
		}
		else
		{
			frequency=0;
			Is_First_Captured = 0;
			frequency_flag = 9;
		}


	}
}


void iHandTask(void *pvParameters )
{
	char send_speed_data[70];

	int ret_flag;
	uint32_t serial_array_size=0;
	ret_flag=0;
	double pedal_avg;
	double temp_var_pedal;
	int i=0;
	float speed_diff=0.0;

	int speed_integer_val=0;

	for(;;)
	{

		pedal_avg=0.0;
		temp_var_pedal=0.0;
		for(i=0;i<10;i++)
		{
			temp_var_pedal = (get_pedal1(hadc1));
			vTaskDelay(pdMS_TO_TICKS(2));
			pedal_avg = pedal_avg + temp_var_pedal;
		}
		pedal_data_percentage = (pedal_avg/10);
		pedal_data_percentage = pedal_data_percentage;

		counter_send_return=counter_send_return + 1;


		if((counter_send_return>SET_SPEED_TIME))
		{
			set_speed_rx_flg=0;
			counter_send_return=0;
			sprintf(send_speed_data,"%03X\n",set_speed);

			vTaskDelay(pdMS_TO_TICKS(1));


			if(inside_pid_loop_flag==9)
			{

				if(set_speed>=speed_rng)
				{
					speed_diff=set_speed-speed_rng;
				}
				else
				{
					speed_diff=speed_rng - set_speed;
				}

				sprintf((char*)send_speed_data,"PID=%u--%3.2f--%3.2f--%3.2f--%3.2f--%3.2f--%lu--%d\n",
										set_speed,speed_rng,frequency,output_pedal_value,error_value,error_percentage,Timer_count,pgmr_pointer);
				log_message(send_speed_data);

			}
			else
			{

				sprintf((char*)send_speed_data,"NOR=%u-%3.2f-%3.2f-%3.2f-%3.2f-%d-%3.2f    ",
														set_speed,speed_rng,frequency,pedal_data_percentage,pedal_disconnection,pgmr_pointer,pid_result);

				log_message(send_speed_data);
				sprintf((char*)send_speed_data,"PID Ctrl Data=%3.3f-%3.3f-%3.3f-%3.3f-%3.3f\n",
										program_setting_data[pgmr_pointer][0],program_setting_data[pgmr_pointer][1],
														program_setting_data[pgmr_pointer][5],program_setting_data[pgmr_pointer][6],
														program_setting_data[pgmr_pointer][7]);
				log_message(send_speed_data);

			}
			vTaskDelay(pdMS_TO_TICKS(3));


		}
		if(speed_signal_limb.st_CFlg==1)
		{
			speed_signal_limb.st_counter++;
			if(speed_signal_limb.st_counter>gps_limp_time)
			{
				speed_signal_limb.st_SFlg=1;
				speed_signal_limb.st_counter=0;
			}
		}
	}
}

uint8_t high_speed_notification=0;
int fw_version_valve = 0;

uint8_t limp_mode=0;
int limp_mode_flag=0;
int trigger_set_speed=0;
int gps_limp_activator=0;
int control_led_status=0;
void SYHandTask( void *pvParameters )
{
	int his_once_chek[6];
	int qqq=0;
	int i=0;
	int valve_buzzer_flag=0;
	int value_enable_flag=0;
	int pid_loop_entry=0;

	int programer_entering_flag=9;
	int buzz_off_flag=0;
	int pedal_exit_flag=0;

	int counter_signal_limp=0;

	char test_arr[25];
	int ret_flag;
	int flag_condition=1;
	uint16_t fz_flag_counter=0;
	int fz_flag_limit=0;
	ret_flag=0;
	uint32_t serial_array_size=0;
	int return_function=0;
	serial_array_size=500;

	store_config_flag=0;
	pid_loop_condition=3;
	pid_loop_entry=1;
	PRINT_DEBUG_TIME = 100;
	HAL_GPIO_WritePin(VALVE_OUT_GPIO_Port, VALVE_OUT_Pin,GPIO_PIN_SET);
	pedal_disconnection=0;
	value_enable_flag=0;
	ivms_uart3_flag=0;

	current_COUNT=DMA_BUFFER_SIZE;
	previous_COUNT = current_COUNT;

	power_limp=0;
	gps_limp=0;



	USBD_CDC_SetTxBuffer(&hUsbDeviceFS, UserTxBuffer, 64, CDC_InstID);
	vTaskDelay(pdMS_TO_TICKS(200));
	HAL_NVIC_EnableIRQ(USB_DRD_FS_IRQn);
	USBD_Start(&hUsbDeviceFS);
	vTaskDelay(pdMS_TO_TICKS(200));



	 store_config_flag=1;

	pgmr_pointer=0;
	data_conversion(pgmr_pointer);


	pgmr_pointer=1;
	data_conversion(pgmr_pointer);

	 pgmr_pointer=1;
	 counter_signal_limp=0;

	 gps_limp_activator=0;

	 gps_limp_time=20;
	 gps_limp_flag=1;
	 control_led_status=0;

	 his_once_chek[0]=eeprom_READBYTE(FROM_ADDR_L);
	 his_once_chek[1]=eeprom_READBYTE(FROM_ADDR_H);
	 his_once_chek[2]=eeprom_READBYTE(TO_ADDR_H);
	 his_once_chek[3]=eeprom_READBYTE(TO_ADDR_H);

	 if((his_once_chek[0]==255)&&(his_once_chek[1]==255)&&(his_once_chek[2]==255)&&(his_once_chek[3]==255))
	 {
		 log_message("\n##Device Started first, clearing...##\n");
		 clear_LOG();
	 }





	for(;;)
	{

		programmer_detection = HAL_GPIO_ReadPin(PGMR_CONE_GPIO_Port, PGMR_CONE_Pin);
		if(programmer_detection==0)
		{
			programer_entering_flag=0;
			HAL_NVIC_DisableIRQ(EXTI2_IRQn);

			vTaskDelay(pdMS_TO_TICKS(50));
			__HAL_UART_ENABLE_IT(&huart2, UART_IT_IDLE);

			vTaskDelay(pdMS_TO_TICKS(100));
			return_function = hand_held_programmer();

			if(return_function==9)
			{

				data_conversion(store_config_flag);

			}
			HAL_NVIC_EnableIRQ(EXTI2_IRQn);
			vTaskDelay(10/portTICK_PERIOD_MS);

			pgmr_pointer=1;
		}
		else
		{
			if(programer_entering_flag==0)
			{
				__HAL_UART_DISABLE_IT(&huart2, UART_IT_IDLE);


				if(store_config_flag == 0)
				{
					store_config_flag=1;
				}
				else if(store_config_flag == 1)
				{
					store_config_flag=1;
				}
			}
			programer_entering_flag=9;

		}


		if(pedal_disconnection<=(program_setting_data[pgmr_pointer][0]+5))
		{
			PRINT_DEBUG_TIME=300;
		}
		else
		{
			PRINT_DEBUG_TIME=100;
		}

		if(frequency_flag==9)
		{
			fz_flag_limit = 0;
		}
		else if(frequency_flag==1)
		{
			fz_flag_limit = 5;
		}

		fz_flag_counter=fz_flag_counter+1;
		if(fz_flag_counter>60000)
		{
			fz_flag_counter=0;
		}

		if(fz_flag_limit==0)
		{
			HAL_GPIO_WritePin(SGL_INDICATION_GPIO_Port, SGL_INDICATION_Pin, GPIO_PIN_RESET);
		}
		else if(fz_flag_counter>=fz_flag_limit)
		{
			if(flag_condition==1)
			{
				flag_condition=0;
				if(control_led_status!=9)
				{
					HAL_GPIO_WritePin(SGL_INDICATION_GPIO_Port, SGL_INDICATION_Pin, GPIO_PIN_SET);
				}
				else
				{
					HAL_GPIO_WritePin(SGL_INDICATION_GPIO_Port, SGL_INDICATION_Pin, GPIO_PIN_RESET);
				}
				fz_flag_counter=0;
			}
			else if(flag_condition==0)
			{
				flag_condition=1;
				if(control_led_status!=9)
				{
					HAL_GPIO_WritePin(SGL_INDICATION_GPIO_Port, SGL_INDICATION_Pin, GPIO_PIN_RESET);
				}
				else
				{
					HAL_GPIO_WritePin(SGL_INDICATION_GPIO_Port, SGL_INDICATION_Pin, GPIO_PIN_RESET);
				}
				fz_flag_counter=0;
			}
		}


		pgmr_pointer=1;


		if(ivms_uart3_flag==1)
		{
			ivms_uart3_flag=0;

			if(can_speedx>5)
			{
				ser_speed = can_speedx;
				set_speed_flag=1;
				serial_speed_receive.st_CFlg=0;
				serial_data_true=1;
				serial_speed_receive.st_counter=0;
			}

		}
		else
		{
			serial_speed_receive.st_CFlg=1;
		}
		if(serial_speed_receive.st_SFlg==1)
		{
			set_speed_flag=0;
			serial_speed_receive.st_SFlg=0;

		}

		if(((set_speed_flag==0) && (cf==0) && (set_speed>=set_speed_eep)) ||((set_speed_flag==0) && (cf==0) && (set_speed<set_speed_eep )))
		{

			set_speed=set_speed_eep;


		}

		else if((set_speed_flag==0) && (cf==1) && (set_speed>=set_speed_eep))
		{



			set_speed=set_speed_eep;


		}
		else if(set_speed_flag==0 && cf==1 && set_speed<set_speed_eep)
		{

			spd_change=1;

		}
		else if(set_speed_flag==1 && cf==0 && set_speed>=ser_speed)
		{

			set_speed=ser_speed;
			high_speed_notification=0;
		}
		else if(set_speed_flag==1 && cf==0 && set_speed<ser_speed)
		{

			set_speed=ser_speed;
			high_speed_notification=0;
		}
		else if(set_speed_flag==1 && cf==1&& set_speed>=ser_speed)
		{

			set_speed=ser_speed;
			high_speed_notification=0;
		}
		else if(set_speed_flag==1 && cf==1&& set_speed<ser_speed)
		{

			spd_change=1;
			high_speed_notification=9;

		}

		if(spd_change==1)
		{
			buzz=1;
		}

		if((buzz && set_speed_flag==0) || spd_change)
		{

			rx_high_set_speed_flag=1;
		}
		else
		{

			rx_high_set_speed_flag=0;
		}





		float ped1=0;
		ped1=pedal_data_percentage;
		limp_mode=0;


		for(int qui=0;qui<4;qui++)
		{
			if(frequency>0)
			{
				speed_rng=getSpeed();
				speed_from_abs = speed_rng;
			}
			else if(gps_limp==9)
			{
				speed_rng = gps_speed_kmhr;
				speed_from_abs=0.0;
			}
			else
			{
				speed_from_abs=0.0;
				speed_rng=0;
			}


			if(gps_limp==9)
			{

				set_speed = 35;

			}
			else if(power_limp==9)
			{
				set_speed = 50;
			}

		float speed_percen=0.0;

		speed_percen = (set_speed * 0.05);
		if(speed_rng>=(set_speed-speed_percen))
		{
			control_buzz_onoff.st_CFlg=1;
			valve_buzzer_flag=9;
		}
		else if(speed_rng<(set_speed-speed_percen))
		{
			control_buzz_onoff.st_CFlg=0;
			valve_buzzer_flag=0;
		}

		if(gps_limp==9)
		{
			g_log_violation_event=true;
			set_speed=35;
			if(gps_limp_activator==0)
			{
				if(speed_rng>=set_speed)
				{
					gps_limp_activator=9;
					HAL_GPIO_WritePin(VALVE_OUT_GPIO_Port, VALVE_OUT_Pin,GPIO_PIN_RESET);
					gps_limp_time = 32;
					gps_limp_flag = 1;
					HAL_GPIO_WritePin(CTRL_INDICATION_GPIO_Port, CTRL_INDICATION_Pin, GPIO_PIN_SET);

					value_enable_flag=9;
					speed_signal_limb.st_CFlg=1;
					speed_signal_limb.st_SFlg=0;
					speed_signal_limb.st_counter=0;
					control_led_status=9;
				}
			}
			else if(speed_rng<(set_speed-1))
			{
				gps_limp_activator=0;
				HAL_GPIO_WritePin(VALVE_OUT_GPIO_Port, VALVE_OUT_Pin,GPIO_PIN_SET);
				HAL_GPIO_WritePin(CTRL_INDICATION_GPIO_Port, CTRL_INDICATION_Pin, GPIO_PIN_RESET);
				value_enable_flag=0;
				speed_signal_limb.st_CFlg=0;
				control_led_status=0;
			}
			if(gps_limp_activator==9)
			{
				if(speed_signal_limb.st_SFlg==1)
				{
					speed_signal_limb.st_SFlg=0;
					speed_signal_limb.st_counter=0;
					HAL_GPIO_WritePin(CTRL_INDICATION_GPIO_Port, CTRL_INDICATION_Pin, GPIO_PIN_SET);
					control_led_status=9;
					if(gps_limp_flag==1)
					{
						gps_limp_flag=0;
						gps_limp_time=20;
						HAL_GPIO_WritePin(VALVE_OUT_GPIO_Port, VALVE_OUT_Pin,GPIO_PIN_RESET);
					}
					else if(gps_limp_flag==0)
					{
						gps_limp_flag=1;
						gps_limp_time=53;
						HAL_GPIO_WritePin(VALVE_OUT_GPIO_Port, VALVE_OUT_Pin,GPIO_PIN_SET);
					}
				}
			}
		}
		else if(power_limp==9)
		{
			g_log_violation_event=true;
			set_speed=50;
			speed_signal_limb.st_CFlg=0;
			if(speed_rng>=set_speed)
			{
				HAL_GPIO_WritePin(VALVE_OUT_GPIO_Port, VALVE_OUT_Pin,GPIO_PIN_RESET);
				HAL_GPIO_WritePin(CTRL_INDICATION_GPIO_Port, CTRL_INDICATION_Pin, GPIO_PIN_SET);
				control_led_status=9;

				value_enable_flag=9;

			}
			else if(speed_rng<=(set_speed-0.2))
			{
				HAL_GPIO_WritePin(VALVE_OUT_GPIO_Port, VALVE_OUT_Pin,GPIO_PIN_SET);
				HAL_GPIO_WritePin(CTRL_INDICATION_GPIO_Port, CTRL_INDICATION_Pin, GPIO_PIN_RESET);
				control_led_status=0;

				value_enable_flag=0;

			}
		}
		else
		{
			speed_signal_limb.st_CFlg=0;
			if(speed_rng>=set_speed)
			{

				HAL_GPIO_WritePin(VALVE_OUT_GPIO_Port, VALVE_OUT_Pin,GPIO_PIN_RESET);
				HAL_GPIO_WritePin(CTRL_INDICATION_GPIO_Port, CTRL_INDICATION_Pin, GPIO_PIN_SET);
				control_led_status=9;

				value_enable_flag=9;

			}
			else if(speed_rng<=(set_speed-0.5))
			{

				HAL_GPIO_WritePin(VALVE_OUT_GPIO_Port, VALVE_OUT_Pin,GPIO_PIN_SET);
				HAL_GPIO_WritePin(CTRL_INDICATION_GPIO_Port, CTRL_INDICATION_Pin, GPIO_PIN_RESET);

				value_enable_flag=0;
				control_led_status=0;

			}
			if(speed_rng>(set_speed+1))
			{
				g_log_violation_event=true;
			}
			else
			{
				g_log_violation_event=false;
			}
		}

		if(set_speed<=50)
		{
			pid_loop_condition=1;
		}
		else
		{
			pid_loop_condition=1;
		}







		if(cf==0)
		{


			I_=0;

			I_Clamping=0;
			if(value_enable_flag!=9)
			{
				HAL_GPIO_WritePin(CTRL_INDICATION_GPIO_Port, CTRL_INDICATION_Pin, GPIO_PIN_RESET);
				control_led_status=0;
			}
			if(speed_rng>=((float)(set_speed-pid_loop_condition)))
			{
				if((speed_rng>=((float)(set_speed-pid_loop_condition)))|| (speed_exit_flag==1))
				{
					i++;

					feedback_pedal(program_setting_data[pgmr_pointer][0]);
					output_pedal_value = program_setting_data[pgmr_pointer][0];

				}
			}
			else
			{
				buzz=0;

				speed_exit_flag=0;
			}
			if(i>=6  && spd_change!=1)
			{
				buzz=!buzz;
				i=0;
			}

			float ped=0;
			ped=pedal_data_percentage;

			if(speed_rng<((float)(set_speed-pid_loop_condition)) && speed_exit_flag==0)
			{
				feedback_pedal(ped);
				output_pedal_value = ped;


			}

			if((((speed_rng>=(set_speed-pid_loop_condition)) && (speed_exit_flag==0)) || ((speed_rng<=set_speed+1) && (speed_exit_flag==1))) && ped>=(program_setting_data[pgmr_pointer][0]))
			{

				cf=1;

			}
		}
		else if(cf==1)
		{
				inside_pid_loop_flag=9;
				PRINT_DEBUG_TIME = 15;

				HAL_GPIO_WritePin(CTRL_INDICATION_GPIO_Port, CTRL_INDICATION_Pin, GPIO_PIN_SET);
				control_led_status=9;
				control_buzz_onoff.st_CFlg=1;
				speed_exit_flag=1;
				i++;
				if(i>=3 && spd_change!=1)
				{
					buzz=!buzz;
					i=0;
				}

				float pedd=0;

				pedd=pedal_data_percentage;
				if(pedd >(program_setting_data[pgmr_pointer][0]+5))
				{
					pid_pedal_exit.st_CFlg=0;
					pedal_exit_flag=1;

				}
				else
				{

					if(gps_limp==9)
					{
						if(speed_rng<=(set_speed-1))
						{
							pid_pedal_exit.st_CFlg=1;
						}
					}
					else
					{
						pid_pedal_exit.st_CFlg=1;
					}
				}
				if(gps_limp==9)
				{
					if(speed_rng<=(set_speed-1))
					{
						pid_pedal_exit.st_CFlg=1;
					}
				}

				if((pid_pedal_exit.st_SFlg==0)&&(pedal_exit_flag==1))
				{

					if(ch_ok)
					{
						ch_ok=0;


						float pp=0;
						float temp=0;

						pp=program_setting_data[pgmr_pointer][7]-program_setting_data[pgmr_pointer][0];
						temp=pp/120.0;
						steady_value=temp*set_speed;


						pp=program_setting_data[pgmr_pointer][0]-temp;


						steady_value=steady_value+pp;
						pid_steady_val=steady_value;


						pp=0;


						pid_return=pid_controller(program_setting_data[pgmr_pointer][3],program_setting_data[pgmr_pointer][4],program_setting_data[pgmr_pointer][5],set_speed,speed_rng,steady_value);









						feedback_pedal(pid_return);
						output_pedal_value = pid_return;


					}
					if(high_speed_notification==9)
					{
						if(pedal_data_percentage<=(program_setting_data[pgmr_pointer][0]+1))
						{

							PRINT_DEBUG_TIME = 100;
							pedal_exit_flag=0;
							pid_pedal_exit.st_CFlg=0;
							pid_pedal_exit.st_SFlg=0;

							if(value_enable_flag!=9)
							{
								HAL_GPIO_WritePin(CTRL_INDICATION_GPIO_Port, CTRL_INDICATION_Pin, GPIO_PIN_RESET);
								control_led_status=0;
							}
							if(valve_buzzer_flag!=9)
							{
								control_buzz_onoff.st_CFlg=0;
							}
							cf=0;

							I_=0;

							I_Clamping=0;
							spd_change=0;
							pgmr_pointer=1;
							qui=9;
							pid_loop_entry=1;


						}
					}
				}
				else
				{
					inside_pid_loop_flag=0;
					PRINT_DEBUG_TIME = 100;
					pedal_exit_flag=0;
					pid_pedal_exit.st_CFlg=0;
					pid_pedal_exit.st_SFlg=0;

					if(value_enable_flag!=9)
					{
						HAL_GPIO_WritePin(CTRL_INDICATION_GPIO_Port, CTRL_INDICATION_Pin, GPIO_PIN_RESET);
						control_led_status=0;
					}
					if(valve_buzzer_flag!=9)
					{
						control_buzz_onoff.st_CFlg=0;
					}
					cf=0;

					I_=0;

					I_Clamping=0;
					spd_change=0;
					pgmr_pointer=1;
					pid_loop_entry=1;
				}

			}
		vTaskDelay(pdMS_TO_TICKS(30));
		}

		if(control_buzz_onoff.st_CFlg==0)
		{
			HAL_GPIO_WritePin(OUTPUT_BUZZER_GPIO_Port, OUTPUT_BUZZER_Pin,GPIO_PIN_RESET);
		}
		else if(control_buzz_onoff.st_SFlg==1)
		{

			if(serial_data_true==0)
			{
				if((rx_high_set_speed_flag==0)||(valve_buzzer_flag==9))
				{
					HAL_GPIO_TogglePin(OUTPUT_BUZZER_GPIO_Port, OUTPUT_BUZZER_Pin);


				}
				else if(rx_high_set_speed_flag==1)
				{
					HAL_GPIO_WritePin(OUTPUT_BUZZER_GPIO_Port, OUTPUT_BUZZER_Pin,GPIO_PIN_SET);

				}
			}
			else
			{
				HAL_GPIO_WritePin(OUTPUT_BUZZER_GPIO_Port, OUTPUT_BUZZER_Pin,GPIO_PIN_RESET);
			}
			control_buzz_onoff.st_SFlg=0;
		}
		if(high_speed_notification==9)
		{
			HAL_GPIO_WritePin(OUTPUT_BUZZER_GPIO_Port, OUTPUT_BUZZER_Pin,GPIO_PIN_SET);
			buzz_off_flag=9;
		}
		else
		{
			if(buzz_off_flag==9)
			{
				buzz_off_flag=0;
				HAL_GPIO_WritePin(OUTPUT_BUZZER_GPIO_Port, OUTPUT_BUZZER_Pin,GPIO_PIN_RESET);
			}

		}


	}
}


void HH_dma( void *pvParameters )
{
	char test_arr[200];
	char localBuf[200];
	int programmer_detection = 0;
	vTaskDelay(pdMS_TO_TICKS(500));

	__HAL_UART_ENABLE_IT(&huart2, UART_IT_IDLE);
	HAL_UARTEx_ReceiveToIdle_DMA(&huart2, handheld_serial_data, DMA_BUFFER_SIZE);


	for(;;)
	{

		HAL_UARTEx_ReceiveToIdle_DMA(&huart2, handheld_serial_data, DMA_BUFFER_SIZE);
        if(xSemaphoreTake(handheld_data_ready_sem, pdMS_TO_TICKS(500)) == pdTRUE)
        {
        	vTaskDelay(pdMS_TO_TICKS(150));

            dma_hand_held.st_CFlg=0;
            dma_hand_held.st_SFlg=9;
            __HAL_UART_CLEAR_IDLEFLAG(&huart2);
            __HAL_UART_CLEAR_FLAG(&huart2, UART_CLEAR_OREF);
            HAL_UARTEx_ReceiveToIdle_DMA(&huart2, handheld_serial_data, DMA_BUFFER_SIZE);
        }
        vTaskDelay(pdMS_TO_TICKS(100));
	}
}





void HAL_TIM_PeriodElapsedCallback(TIM_HandleTypeDef *htim)
{

  if (htim->Instance == TIM5)
  {
    HAL_IncTick();
  }

}


void Error_Handler(void)
{

}
#ifdef USE_FULL_ASSERT

void assert_failed(uint8_t *file, uint32_t line)
{

}
#endif
