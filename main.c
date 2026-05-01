#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "cJSON.h"

#include <sys/time.h>

// --- 1. ESTRUTURAS DE DADOS ---
typedef struct {
    char cidade[50];
    double temperatura;
    double umidade;
    double pressao;
    double bateria;
    int sf;
    char data_hora[30]; // Guarda a data da temperatura para facilitar depois
} Medicao;

#define TAMANHO_BUFFER 20000
Medicao buffer_medicoes[TAMANHO_BUFFER];
int total_no_buffer = 0;
int leitura_concluida = 0; 

// --- 2. VARIÁVEIS DE SINCRONIZAÇÃO (MUTEX) ---
pthread_mutex_t mutex_buffer = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_pode_produzir = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond_pode_consumir = PTHREAD_COND_INITIALIZER;

// Função auxiliar para ler arquivo para a memória
char* ler_arquivo(const char* nome_arquivo) {
    FILE *f = fopen(nome_arquivo, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long tamanho = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *conteudo = malloc(tamanho + 1);
    fread(conteudo, 1, tamanho, f);
    conteudo[tamanho] = '\0';
    fclose(f);
    return conteudo;
}

// Função que processa um arquivo e joga no Buffer
void processar_arquivo_json(const char* nome_arquivo) {
    char *conteudo_json = ler_arquivo(nome_arquivo);
    if (!conteudo_json) {
        printf("Erro ao ler %s\n", nome_arquivo);
        return;
    }

    cJSON *json_array = cJSON_Parse(conteudo_json);
    if (!json_array) {
        free(conteudo_json);
        return;
    }

    int total_itens = cJSON_GetArraySize(json_array);
    for (int i = 0; i < total_itens; i++) {
        cJSON *item = cJSON_GetArrayItem(json_array, i);
        
        // Trata a diferença entre os dois arquivos
        cJSON *texto_interno = cJSON_GetObjectItem(item, "brute_data");
        if (!texto_interno) texto_interno = cJSON_GetObjectItem(item, "payload");
        
        if (texto_interno && cJSON_IsString(texto_interno)) {
            cJSON *inner_json = cJSON_Parse(texto_interno->valuestring);
            if (inner_json) {
                cJSON *device_name = cJSON_GetObjectItem(inner_json, "device_name");
                cJSON *data_array = cJSON_GetObjectItem(inner_json, "data");
                
                if (device_name && data_array && cJSON_IsArray(data_array)) {
                    Medicao m;
                    // Inicializa com valores impossíveis para detetar pacotes incompletos
                    m.temperatura = -999.0;
                    m.umidade = -999.0;
                    m.pressao = -999.0;
                    m.bateria = -999.0;
                    m.sf = -1;
                    strcpy(m.data_hora, "Sem Data");
                    strncpy(m.cidade, device_name->valuestring, 49);
                    
                    // Varre as variáveis dentro do "data"
                    int qtd_dados = cJSON_GetArraySize(data_array);
                    for (int j = 0; j < qtd_dados; j++) {
                        cJSON *dado = cJSON_GetArrayItem(data_array, j);
                        cJSON *var = cJSON_GetObjectItem(dado, "variable");
                        cJSON *val = cJSON_GetObjectItem(dado, "value");
                        cJSON *time = cJSON_GetObjectItem(dado, "time");
                        
                        if (var && val) {
                            if (strcmp(var->valuestring, "temperature") == 0) {
                                m.temperatura = (val->type == cJSON_Number) ? val->valuedouble : val->valueint;
                                if (time) strncpy(m.data_hora, time->valuestring, 29);
                            } else if (strcmp(var->valuestring, "humidity") == 0) {
                                m.umidade = (val->type == cJSON_Number) ? val->valuedouble : val->valueint;
                            } else if (strcmp(var->valuestring, "airpressure") == 0) {
                                m.pressao = (val->type == cJSON_Number) ? val->valuedouble : val->valueint;
                            } else if (strcmp(var->valuestring, "batterylevel") == 0) {
                                m.bateria = (val->type == cJSON_Number) ? val->valuedouble : val->valueint;
                            } else if (strcmp(var->valuestring, "lora_spreading_factor") == 0) {
                                m.sf = val->valueint;
                            }
                        }
                    }
                    
                    // --- ZONA CRÍTICA: INSERIR NO BUFFER ---
                    pthread_mutex_lock(&mutex_buffer);
                    while (total_no_buffer == TAMANHO_BUFFER) {
                        pthread_cond_wait(&cond_pode_produzir, &mutex_buffer);
                    }
                    
                    buffer_medicoes[total_no_buffer] = m;
                    total_no_buffer++;
                    
                    pthread_cond_signal(&cond_pode_consumir);
                    pthread_mutex_unlock(&mutex_buffer);
                    // ---------------------------------------
                }
                cJSON_Delete(inner_json);
            }
        }
    }
    cJSON_Delete(json_array);
    free(conteudo_json);
}

// --- 3. THREAD DE LEITURA (PRODUTORA) ---
void* thread_leitura(void* arg) {
    printf("[Produtor] Lendo senzemo_cx_bg.json...\n");
    processar_arquivo_json("senzemo_cx_bg.json");
    
    printf("[Produtor] Lendo mqtt_senzemo_cx_bg.json...\n");
    processar_arquivo_json("mqtt_senzemo_cx_bg.json");

    // Avisa que terminou
    pthread_mutex_lock(&mutex_buffer);
    leitura_concluida = 1;
    pthread_cond_broadcast(&cond_pode_consumir);
    pthread_mutex_unlock(&mutex_buffer);
    
    printf("[Produtor] Finalizou a leitura de todos os arquivos.\n");
    pthread_exit(NULL);
}

// --- ESTRUTURA PARA GUARDAR OS CÁLCULOS ---
typedef struct {
    int contagem;
    
    // Temperatura
    double min_temp, max_temp, soma_temp;
    char data_min_temp[30], data_max_temp[30];
    
    // Umidade
    double min_umi, max_umi, soma_umi;
    char data_min_umi[30], data_max_umi[30];
    
    // Pressão
    double min_pres, max_pres, soma_pres;
    char data_min_pres[30], data_max_pres[30];
    
    // Bateria
    double bat_inicial, bat_final;
    int bat_registrada; // Flag para saber se já pegamos a 1ª bateria
    
    // Spreading Factor (SF varia de 7 a 12 no LoRa)
    int sf_usados[13]; 
} Estatisticas;

Estatisticas est_caxias = {0};
Estatisticas est_bento = {0};

// Função auxiliar para atualizar as estatísticas de uma cidade
void atualizar_estatisticas(Estatisticas *est, Medicao m) {
    // Inicializa os mínimos com valores altíssimos na primeira vez
    if (est->contagem == 0) {
        est->min_temp = 999.0; est->max_temp = -999.0;
        est->min_umi = 999.0; est->max_umi = -999.0;
        est->min_pres = 9999.0; est->max_pres = -999.0;
    }

    // Processa Temperatura (só se for válida)
    if (m.temperatura != -999.0) {
        if (m.temperatura > est->max_temp) { est->max_temp = m.temperatura; strcpy(est->data_max_temp, m.data_hora); }
        if (m.temperatura < est->min_temp) { est->min_temp = m.temperatura; strcpy(est->data_min_temp, m.data_hora); }
        est->soma_temp += m.temperatura;
    }

    // Processa Umidade
    if (m.umidade != -999.0) {
        if (m.umidade > est->max_umi) { est->max_umi = m.umidade; strcpy(est->data_max_umi, m.data_hora); }
        if (m.umidade < est->min_umi) { est->min_umi = m.umidade; strcpy(est->data_min_umi, m.data_hora); }
        est->soma_umi += m.umidade;
    }

    // Processa Pressão
    if (m.pressao != -999.0) {
        if (m.pressao > est->max_pres) { est->max_pres = m.pressao; strcpy(est->data_max_pres, m.data_hora); }
        if (m.pressao < est->min_pres) { est->min_pres = m.pressao; strcpy(est->data_min_pres, m.data_hora); }
        est->soma_pres += m.pressao;
    }

    // Processa Bateria
    if (m.bateria != -999.0) {
        if (est->bat_registrada == 0) {
            est->bat_inicial = m.bateria; 
            est->bat_registrada = 1;
        }
        est->bat_final = m.bateria;
    }

    // Processa Spreading Factor
    if (m.sf >= 7 && m.sf <= 12) {
        est->sf_usados[m.sf] = 1; 
    }

    est->contagem++;
}

// --- 4. THREAD DE ESTATÍSTICAS (CONSUMIDORA) ---
void* thread_estatisticas(void* arg) {
    while (1) {
        pthread_mutex_lock(&mutex_buffer);
        
        while (total_no_buffer == 0 && !leitura_concluida) {
            pthread_cond_wait(&cond_pode_consumir, &mutex_buffer);
        }
        
        if (total_no_buffer == 0 && leitura_concluida) {
            pthread_mutex_unlock(&mutex_buffer);
            break;
        }
        
        total_no_buffer--;
        Medicao m = buffer_medicoes[total_no_buffer];
        
        pthread_cond_signal(&cond_pode_produzir);
        pthread_mutex_unlock(&mutex_buffer);
        
        // Verifica de qual cidade é a medição usando a função strstr
        if (strstr(m.cidade, "Caxias") != NULL) {
            atualizar_estatisticas(&est_caxias, m);
        } else if (strstr(m.cidade, "Bento") != NULL) {
            atualizar_estatisticas(&est_bento, m);
        }
    }
    
    printf("[Consumidor] Estatisticas calculadas com sucesso!\n");
    pthread_exit(NULL);
}

// --- 4.5 THREAD DE LOGS (AUDITORIA) ---
void* thread_logs(void* arg) {
    FILE *f_log = fopen("log_auditoria.txt", "w");
    if (!f_log) {
        printf("Erro ao criar arquivo de log.\n");
        pthread_exit(NULL);
    }

    fprintf(f_log, "=== INICIO DA AUDITORIA ===\n");

    while (1) {
        // Usa o Mutex rápido apenas para ler o estado atual das variáveis com segurança
        pthread_mutex_lock(&mutex_buffer);
        int flag_concluida = leitura_concluida;
        int buffer_atual = total_no_buffer;
        int count_cx = est_caxias.contagem;
        int count_bg = est_bento.contagem;
        pthread_mutex_unlock(&mutex_buffer);

        fprintf(f_log, "[LOG] Itens aguardando no Buffer: %d | Medicoes processadas -> Caxias: %d, Bento: %d\n", 
                buffer_atual, count_cx, count_bg);
        fflush(f_log); // Força a gravação no disco imediatamente

        // Se a leitura acabou e o buffer esvaziou, o trabalho terminou
        if (flag_concluida && buffer_atual == 0) {
            break;
        }

        // Dorme por 10 milissegundos (0.01 segundos) para não gerar um arquivo gigante e não travar a CPU
        struct timespec ts = {0, 10000000L};
        nanosleep(&ts, NULL);
    }

    fprintf(f_log, "=== FIM DA AUDITORIA - PROCESSAMENTO CONCLUIDO ===\n");
    fclose(f_log);
    pthread_exit(NULL);
}

// --- 5. FUNÇÃO DE IMPRESSÃO (TEMPLATE) ---
void imprimir_resultados(double tempo_gasto) {
    printf("\n============================================================\n");
    printf("ANALISE DE DADOS DOS SENSORES - CityLivingLab\n");
    printf("Processamento utilizando pthreads\n");
    printf("============================================================\n\n");

    printf("Total de registros processados Caxias: %d\n", est_caxias.contagem);
    printf("Total de registros processados Bento: %d\n\n", est_bento.contagem);

    // --- TEMPERATURA ---
    printf("------------------------------------------------------------\n");
    printf("TEMPERATURA (C)\n");
    printf("------------------------------------------------------------\n");
    printf("Cidade            | Minima | Data/Hora             | Maxima | Data/Hora             | Media\n");
    printf("-----------------------------------------------------------------------------------------------\n");
    printf("Caxias do Sul     | %-6.2f | %-21s | %-6.2f | %-21s | %.2f\n",
           est_caxias.min_temp, est_caxias.data_min_temp, est_caxias.max_temp, est_caxias.data_max_temp, est_caxias.soma_temp / est_caxias.contagem);
    printf("Bento Goncalves   | %-6.2f | %-21s | %-6.2f | %-21s | %.2f\n\n",
           est_bento.min_temp, est_bento.data_min_temp, est_bento.max_temp, est_bento.data_max_temp, est_bento.soma_temp / est_bento.contagem);

    // --- UMIDADE ---
    printf("------------------------------------------------------------\n");
    printf("UMIDADE (%%)\n");
    printf("------------------------------------------------------------\n");
    printf("Cidade            | Minima | Data/Hora             | Maxima | Data/Hora             | Media\n");
    printf("-----------------------------------------------------------------------------------------------\n");
    printf("Caxias do Sul     | %-6.2f | %-21s | %-6.2f | %-21s | %.2f\n",
           est_caxias.min_umi, est_caxias.data_min_umi, est_caxias.max_umi, est_caxias.data_max_umi, est_caxias.soma_umi / est_caxias.contagem);
    printf("Bento Goncalves   | %-6.2f | %-21s | %-6.2f | %-21s | %.2f\n\n",
           est_bento.min_umi, est_bento.data_min_umi, est_bento.max_umi, est_bento.data_max_umi, est_bento.soma_umi / est_bento.contagem);

    // --- PRESSAO ---
    printf("------------------------------------------------------------\n");
    printf("PRESSAO ATMOSFERICA (hPa)\n");
    printf("------------------------------------------------------------\n");
    printf("Cidade            | Minima | Data/Hora             | Maxima | Data/Hora             | Media\n");
    printf("-----------------------------------------------------------------------------------------------\n");
    printf("Caxias do Sul     | %-6.2f | %-21s | %-6.2f | %-21s | %.2f\n",
           est_caxias.min_pres, est_caxias.data_min_pres, est_caxias.max_pres, est_caxias.data_max_pres, est_caxias.soma_pres / est_caxias.contagem);
    printf("Bento Goncalves   | %-6.2f | %-21s | %-6.2f | %-21s | %.2f\n\n",
           est_bento.min_pres, est_bento.data_min_pres, est_bento.max_pres, est_bento.data_max_pres, est_bento.soma_pres / est_bento.contagem);

    // --- BATERIA ---
    printf("------------------------------------------------------------\n");
    printf("BATERIA\n");
    printf("------------------------------------------------------------\n");
    printf("Cidade            | Inicial (V) | Final (V) | Consumo (V)\n");
    printf("------------------------------------------------------------\n");
    printf("Caxias do Sul     | %-11.2f | %-9.2f | %.2f\n", est_caxias.bat_inicial, est_caxias.bat_final, est_caxias.bat_inicial - est_caxias.bat_final);
    printf("Bento Goncalves   | %-11.2f | %-9.2f | %.2f\n\n", est_bento.bat_inicial, est_bento.bat_final, est_bento.bat_inicial - est_bento.bat_final);

    // --- SPREADING FACTORS ---
    printf("------------------------------------------------------------\n");
    printf("SPREADING FACTORS UTILIZADOS\n");
    printf("------------------------------------------------------------\n");
    printf("Cidade            | SF utilizados\n");
    printf("------------------------------------------------------------\n");
    
    printf("Caxias do Sul     | ");
    int primeiro = 1;
    for(int i = 7; i <= 12; i++) {
        if(est_caxias.sf_usados[i]) {
            if(!primeiro) printf(", ");
            printf("SF%d", i);
            primeiro = 0;
        }
    }
    printf("\n");

    printf("Bento Goncalves   | ");
    primeiro = 1;
    for(int i = 7; i <= 12; i++) {
        if(est_bento.sf_usados[i]) {
            if(!primeiro) printf(", ");
            printf("SF%d", i);
            primeiro = 0;
        }
    }
    printf("\n\n");

    // --- DESEMPENHO ---
    printf("------------------------------------------------------------\n");
    printf("DESEMPENHO\n");
    printf("------------------------------------------------------------\n");
    printf("Tempo total de execucao: %.2f segundos\n", tempo_gasto);
    printf("Threads utilizadas: 3 (Leitura, Estatisticas, Logs)\n");
    printf("============================================================\n");
}

int main() {
    struct timeval inicio, fim;
    gettimeofday(&inicio, NULL);

    // Agora temos 3 threads
    pthread_t t_leitura, t_estatisticas, t_logs;
    
    // Inicia as threads concorrentes
    pthread_create(&t_leitura, NULL, thread_leitura, NULL);
    pthread_create(&t_estatisticas, NULL, thread_estatisticas, NULL);
    pthread_create(&t_logs, NULL, thread_logs, NULL); // Nova thread iniciada
    
    // Aguarda todas terminarem
    pthread_join(t_leitura, NULL);
    pthread_join(t_estatisticas, NULL);
    pthread_join(t_logs, NULL); // Aguarda a thread de logs fechar o arquivo
    
    gettimeofday(&fim, NULL);
    double tempo_gasto = (fim.tv_sec - inicio.tv_sec) +
                         (fim.tv_usec - inicio.tv_usec) / 1000000.0;

    imprimir_resultados(tempo_gasto);

    return 0;
}