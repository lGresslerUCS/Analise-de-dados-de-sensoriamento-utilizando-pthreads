#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include "cJSON.h"
#include <sys/time.h>

#define TAMANHO_BUFFER 20000

#define MAX_TIMESTAMPS 120000

char timestamps_vistos[MAX_TIMESTAMPS][30];
int total_timestamps = 0;

pthread_mutex_t mutex_timestamps = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    char cidade[50];
    double temperatura;
    double umidade;
    double pressao;
    double bateria;
    int sf;
    char data_hora[30];
} Medicao;

typedef struct {

    int contagem;

    double min_temp, max_temp, soma_temp;
    char data_min_temp[30], data_max_temp[30];

    double min_umi, max_umi, soma_umi;
    char data_min_umi[30], data_max_umi[30];

    double min_pres, max_pres, soma_pres;
    char data_min_pres[30], data_max_pres[30];

    double bat_inicial, bat_final;
    int bat_registrada;

    int sf_usados[13];

} Estatisticas;


Medicao buffer_medicoes[TAMANHO_BUFFER];

int total_no_buffer = 0;
int leitura_concluida = 0;

pthread_mutex_t mutex_buffer = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_pode_produzir = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond_pode_consumir = PTHREAD_COND_INITIALIZER;

Estatisticas est_caxias = {0};
Estatisticas est_bento = {0};

int eh_duplicata(const char *timestamp){

    pthread_mutex_lock(&mutex_timestamps);

    for(int i=0;i<total_timestamps;i++){
        if(strcmp(timestamps_vistos[i], timestamp) == 0){
            pthread_mutex_unlock(&mutex_timestamps);
            return 1;
        }
    }

    if(total_timestamps < MAX_TIMESTAMPS){
        strcpy(timestamps_vistos[total_timestamps++], timestamp);
    }

    pthread_mutex_unlock(&mutex_timestamps);

    return 0;
}


void inicializar_estatisticas(Estatisticas *est) {

    est->min_temp = 999;
    est->max_temp = -999;

    est->min_umi = 999;
    est->max_umi = -999;

    est->min_pres = 9999;
    est->max_pres = -999;
}

void atualizar_min_max(
        double valor,
        double *min,
        double *max,
        double *soma,
        char *data_min,
        char *data_max,
        const char *data_atual
) {

    if(valor > *max){
        *max = valor;
        strcpy(data_max, data_atual);
    }

    if(valor < *min){
        *min = valor;
        strcpy(data_min, data_atual);
    }

    *soma += valor;
}

const char* cidade_de_medicao(Medicao *m){

    if(strstr(m->cidade,"Caxias"))
        return "CAXIAS";

    if(strstr(m->cidade,"Bento"))
        return "BENTO";

    return "DESCONHECIDA";
}


char* ler_arquivo(const char* nome_arquivo){

    FILE *f = fopen(nome_arquivo,"rb");

    if(!f) return NULL;

    fseek(f,0,SEEK_END);
    long tamanho = ftell(f);
    fseek(f,0,SEEK_SET);

    char *conteudo = malloc(tamanho+1);

    fread(conteudo,1,tamanho,f);

    conteudo[tamanho]='\0';

    fclose(f);

    return conteudo;
}


void processar_arquivo_json(const char* nome_arquivo){

    char *conteudo = ler_arquivo(nome_arquivo);

    if(!conteudo){
        printf("Erro ao ler %s\n",nome_arquivo);
        return;
    }

    cJSON *array = cJSON_Parse(conteudo);

    if(!array){
        free(conteudo);
        return;
    }

    int total = cJSON_GetArraySize(array);

    for(int i=0;i<total;i++){

        cJSON *item = cJSON_GetArrayItem(array,i);

        cJSON *texto = cJSON_GetObjectItem(item,"brute_data");
        if(!texto)
            texto = cJSON_GetObjectItem(item,"payload");

        if(texto && cJSON_IsString(texto)){

            cJSON *inner = cJSON_Parse(texto->valuestring);

            if(inner){

                Medicao m;

                m.temperatura = -999;
                m.umidade = -999;
                m.pressao = -999;
                m.bateria = -999;
                m.sf = -1;

                strcpy(m.data_hora,"Sem Data");

                cJSON *device = cJSON_GetObjectItem(inner,"device_name");
                cJSON *dados = cJSON_GetObjectItem(inner,"data");

                if(device){
                    strncpy(m.cidade,device->valuestring,49);
                    m.cidade[49] = '\0';
                }

                if(dados && cJSON_IsArray(dados)){

                    int qtd = cJSON_GetArraySize(dados);

                    for(int j=0;j<qtd;j++){

                        cJSON *dado = cJSON_GetArrayItem(dados,j);

                        cJSON *var = cJSON_GetObjectItem(dado,"variable");
                        cJSON *val = cJSON_GetObjectItem(dado,"value");
                        cJSON *time = cJSON_GetObjectItem(dado,"time");

                        if(!var || !val) continue;

                        double valor = val->valuedouble;

                        if(strcmp(var->valuestring,"temperature")==0){

                            m.temperatura = valor;

                            if(time){
                                strncpy(m.data_hora,time->valuestring,29);
                                m.data_hora[29] = '\0';
                            }

                        }
                        else if(strcmp(var->valuestring,"humidity")==0)
                            m.umidade = valor;

                        else if(strcmp(var->valuestring,"airpressure")==0)
                            m.pressao = valor;

                        else if(strcmp(var->valuestring,"batterylevel")==0)
                            m.bateria = valor;

                        else if(strcmp(var->valuestring,"lora_spreading_factor")==0)
                            m.sf = val->valueint;

                    }
                    if(!eh_duplicata(m.data_hora)){

                        pthread_mutex_lock(&mutex_buffer);

                        while(total_no_buffer==TAMANHO_BUFFER)
                            pthread_cond_wait(&cond_pode_produzir,&mutex_buffer);

                        buffer_medicoes[total_no_buffer++] = m;

                        pthread_cond_signal(&cond_pode_consumir);

                        pthread_mutex_unlock(&mutex_buffer);
                    }

                }

                cJSON_Delete(inner);
            }
        }
    }

    cJSON_Delete(array);
    free(conteudo);
}


void* thread_leitura(void* arg){

    processar_arquivo_json("senzemo_cx_bg.json");
    processar_arquivo_json("mqtt_senzemo_cx_bg.json");

    pthread_mutex_lock(&mutex_buffer);

    leitura_concluida = 1;

    pthread_cond_broadcast(&cond_pode_consumir);

    pthread_mutex_unlock(&mutex_buffer);

    return NULL;
}


void atualizar_estatisticas(Estatisticas *est, Medicao m){

    if(est->contagem == 0)
        inicializar_estatisticas(est);

    if(m.temperatura != -999)
        atualizar_min_max(
            m.temperatura,
            &est->min_temp,
            &est->max_temp,
            &est->soma_temp,
            est->data_min_temp,
            est->data_max_temp,
            m.data_hora
        );

    est->contagem++;
}


void* thread_estatisticas(void* arg){

    while(1){

        pthread_mutex_lock(&mutex_buffer);

        while(total_no_buffer==0 && !leitura_concluida)
            pthread_cond_wait(&cond_pode_consumir,&mutex_buffer);

        if(total_no_buffer==0 && leitura_concluida){

            pthread_mutex_unlock(&mutex_buffer);
            break;
        }

        Medicao m = buffer_medicoes[--total_no_buffer];

        pthread_cond_signal(&cond_pode_produzir);

        pthread_mutex_unlock(&mutex_buffer);

        const char *cidade = cidade_de_medicao(&m);

        if(strcmp(cidade,"CAXIAS")==0)
            atualizar_estatisticas(&est_caxias,m);

        else if(strcmp(cidade,"BENTO")==0)
            atualizar_estatisticas(&est_bento,m);

    }

    return NULL;
}


void* thread_logs(void* arg){

    FILE *f = fopen("processamento.log","w");

    while(1){

        pthread_mutex_lock(&mutex_buffer);

        int fim = leitura_concluida;
        int buffer = total_no_buffer;

        int cx = est_caxias.contagem;
        int bg = est_bento.contagem;

        pthread_mutex_unlock(&mutex_buffer);

        fprintf(f,"Buffer:%d | Caxias:%d | Bento:%d\n",buffer,cx,bg);
        fflush(f);

        if(fim && buffer==0)
            break;

        usleep(10000);
    }

    fclose(f);

    return NULL;
}


void imprimir_resultados(double tempo){

    printf("\n=================================================\n");
    printf("ANALISE DE DADOS DOS SENSORES\n");
    printf("=================================================\n\n");

    printf("Total Caxias: %d\n",est_caxias.contagem);
    printf("Total Bento : %d\n",est_bento.contagem);

    if(est_caxias.contagem>0)
        printf("\nTemp media Caxias: %.2f\n",est_caxias.soma_temp/est_caxias.contagem);

    if(est_bento.contagem>0)
        printf("Temp media Bento: %.2f\n",est_bento.soma_temp/est_bento.contagem);

    printf("\nTempo total: %.2f segundos\n",tempo);
}


int main() {

    struct timeval inicio, fim;
    gettimeofday(&inicio, NULL);

    pthread_t t_leitura, t_estatisticas, t_logs;

    pthread_create(&t_leitura, NULL, thread_leitura, NULL);
    pthread_create(&t_estatisticas, NULL, thread_estatisticas, NULL);
    pthread_create(&t_logs, NULL, thread_logs, NULL);

    pthread_join(t_leitura, NULL);
    pthread_join(t_estatisticas, NULL);
    pthread_join(t_logs, NULL);

    gettimeofday(&fim, NULL);

    double tempo_gasto = (fim.tv_sec - inicio.tv_sec) +
                         (fim.tv_usec - inicio.tv_usec) / 1000000.0;

    imprimir_resultados(tempo_gasto);

    return 0;
}