
#include <droplet.h>
#include <droplet/backend.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>
#include <math.h>
#include "common.h"

char *profile = NULL;
dpl_ctx_t *ctx = NULL;
char *bucket = "";
int Rflag = 0; //random content
int zflag = 0; //set 'z' in content
int rflag = 0; //use random size
int vflag = 0; //verbose
int oflag = 0; //random oids
int max_block_size = 30000;
int n_ops = 1; //per thread
int n_threads = 10;
int timebase = 1; //seconds
dpl_storage_class_t class = DPL_STORAGE_CLASS_STANDARD;
int send_usermd = 1;
int maxreads = 100;
int print_running_stats = 1;
int print_gnuplot_stats = 0;
int seed = 0;

struct drand48_data drbuffer;

pthread_attr_t g_detached_thread_attr, g_joinable_thread_attr;

static pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;

static pthread_t *workers = NULL;

typedef struct _stats
{
  uint64_t n_failures;
  uint64_t n_success;
  uint64_t n_bytes;
  double latency_sq_sum;
  uint64_t latency_sum;
} tstats;

tstats stats_get;
tstats stats_put;
tstats stats_delete;

typedef struct
{
  uint32_t id;
} tthreadid;

void test_cleanup(void *arg)
{
  tthreadid *threadid = (tthreadid *)arg;

  if (vflag > 1)
    fprintf(stderr, "cleanup %u\n", threadid->id);
}

/*
 * Test thread
 */

static void *test_main(void *arg)
{
  tthreadid *threadid = (tthreadid *)arg;
  int i, ret;
  int dummy;
  char *block;
  size_t block_size;

  pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &dummy);
  pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, &dummy);

  pthread_cleanup_push(test_cleanup, arg);

  for (i = 0; i < n_ops;i++)
    {
      struct timeval tv1, tv2;
      dpl_dict_t *metadata = NULL;
      dpl_sysmd_t sysmd;
#define PATTERN_LEN 32
      char pattern[PATTERN_LEN+1];

      pthread_testcancel();

      if (rflag)
          block_size = rand() % max_block_size;
      else
          block_size = max_block_size;

      if (vflag)
          fprintf(stderr, "post_id block_size %llu\n",
                  (unsigned long long)block_size);

      block = malloc(block_size);
      if (NULL == block)
        {
          perror("malloc");
          exit(1);
        }

      dpltest_rand_str(pattern, PATTERN_LEN);
      
      //fprintf(stderr, "pattern=%s\n", pattern);

      if (Rflag)
          dpltest_gen_data(pattern, block, block_size);
      else if (zflag)
          memset(block, 'z', block_size);
      else
          memset(block, 0, block_size);

      if (send_usermd)
        {
          metadata = dpl_dict_new(13);
          assert(NULL != metadata);
          ret = dpl_dict_add(metadata, "foo", "bar", 1);
          assert(DPL_SUCCESS == ret);
        }

      gettimeofday(&tv1, NULL);

      //ret = write(1, block, block_size);

      ret = dpl_post_id(ctx, bucket, NULL, NULL, DPL_FTYPE_REG, NULL, NULL, metadata, NULL, 
                        block, block_size, NULL, &sysmd);

      gettimeofday(&tv2, NULL);
      
      if (vflag)
        fprintf(stderr, "id=%s\n", sysmd.id);

      pthread_mutex_lock(&stats_lock);
      if (0 == ret)
        {
          uint64_t latency = (tv2.tv_sec - tv1.tv_sec)*1000 + (tv2.tv_usec - tv1.tv_usec) / 1000;
          stats_put.n_success++;
          stats_put.latency_sum += latency;
          stats_put.latency_sq_sum += (double) latency*latency;
          stats_put.n_bytes += block_size;
        }
      else
        {
          stats_put.n_failures++;
        }
      pthread_mutex_unlock(&stats_lock);

      if (NULL != metadata)
        dpl_dict_free(metadata);
      
      metadata = NULL;

      /* this is a hack to do reads */
      int j = maxreads;
      int nbtodo = 0;

      while (j >= 100)
        {
          nbtodo ++;
          j -= 100;
        }

      if (j > 0 && rand() % 100 < j)
        {
          nbtodo ++;
        }

      for (j = 0 ; j < nbtodo ; j ++)
        {
          char *data_buf;
          u32 data_size;

          data_buf = NULL;

          gettimeofday(&tv1, NULL);
          ret = dpl_get_id(ctx, bucket, sysmd.id, NULL, DPL_FTYPE_REG, NULL, NULL, &data_buf, &data_size, &metadata, NULL);
          gettimeofday(&tv2, NULL);

          if (0 == ret && Rflag)
            {
              //ret = write(1, data_buf, data_size);
              if (0 != dpltest_check_data(pattern, data_buf, data_size))
                {
                  fprintf(stderr, "bad content\n");
                  exit(1);
                }
            }

          pthread_mutex_lock(&stats_lock);
          if (0 == ret)
            {
              uint64_t latency = (tv2.tv_sec - tv1.tv_sec)*1000 + (tv2.tv_usec - tv1.tv_usec) / 1000;
              stats_get.n_success++;
              stats_get.latency_sum += latency;
              stats_get.latency_sq_sum += (double) latency*latency;
              stats_get.n_bytes += data_size;
            }
          else
            {
              stats_get.n_failures++;
            }
          pthread_mutex_unlock(&stats_lock);

          if (NULL != data_buf)
            free(data_buf);
          if (NULL != metadata)
            dpl_dict_free(metadata);
        }

      gettimeofday(&tv1, NULL);
      ret = dpl_delete_id(ctx, bucket, sysmd.id, NULL, DPL_FTYPE_REG, NULL);
      gettimeofday(&tv2, NULL);

      pthread_mutex_lock(&stats_lock);
      if (0 == ret)
        {
          uint64_t latency = (tv2.tv_sec - tv1.tv_sec)*1000 + (tv2.tv_usec - tv1.tv_usec) / 1000;
          stats_delete.n_success++;
          stats_delete.latency_sum += latency;
          stats_delete.latency_sq_sum += (double) latency*latency;
        }
      else
        {
          stats_delete.n_failures++;
        }
      pthread_mutex_unlock(&stats_lock);

      free(block);
    }

  if (vflag)
      fprintf(stderr, "%u done\n", threadid->id);

  pthread_cleanup_pop(0);

  pthread_exit(NULL);
}

typedef struct _tmainstats
{
  int i; /* loop */
  struct timeval tv1;
  struct timeval tv2;
  uint64_t speed;
  uint64_t total_speed;
} tmainstats;

tmainstats mainstats;

void print_stats_put_nolock(void)
{
  double lat_sigma_2 = 0;
  double lat_avg = 0;

  if (stats_put.n_success > 0)
    {
      lat_avg = (double) stats_put.latency_sum / stats_put.n_success;
      lat_sigma_2 = sqrt(stats_put.latency_sq_sum / stats_put.n_success - lat_avg*lat_avg);
    }


  printf("time=%lu ok=%llu fail=%llu bytes=%llu (%lluMbit/s) aver %lluMbit/s latency: %.1f ms sd: %.1f ops: %.2f (p)\n",
          mainstats.tv2.tv_sec - mainstats.tv1.tv_sec,
          (unsigned long long)stats_put.n_success,
          (unsigned long long)stats_put.n_failures,
          (unsigned long long)stats_put.n_bytes,
          (unsigned long long)mainstats.speed/timebase,
          (unsigned long long)mainstats.total_speed/(mainstats.i*timebase),
          lat_avg,
          lat_sigma_2,
          ((float)(mainstats.tv2.tv_sec - mainstats.tv1.tv_sec) * 1000 + (mainstats.tv2.tv_usec - mainstats.tv1.tv_usec)/1000) > 0 ? (float) stats_put.n_success / (((float)(mainstats.tv2.tv_sec - mainstats.tv1.tv_sec) * 1000 + (mainstats.tv2.tv_usec - mainstats.tv1.tv_usec)/1000) / 1000.0) : 0);

  return;
}

void print_gpstats_put_nolock(void)
{
  double lat_sigma_2 = 0;
  double lat_avg = 0;
  double total_time_ms;

  if (stats_put.n_success > 0)
    {
      lat_avg = (double) stats_put.latency_sum / stats_put.n_success;
      lat_sigma_2 = sqrt(stats_put.latency_sq_sum / stats_put.n_success - lat_avg*lat_avg);
    }

  total_time_ms = (double) (mainstats.tv2.tv_sec - mainstats.tv1.tv_sec) * 1000.0 + (mainstats.tv2.tv_usec - mainstats.tv1.tv_usec) / 1000.0;

  /* (d/p/g) nt class size time latency sd ops*/

  printf("# 1 n_threads class max_block_size seed time n_succ n_fail lat lat_sd ops\n");
  printf("1 ");
  printf("%d ", n_threads);
  printf("%d ", class);
  printf("%d ", max_block_size);
  printf("%d ", seed);
  printf("%lu ", mainstats.tv2.tv_sec - mainstats.tv1.tv_sec);
  printf("%lu %lu ", (long unsigned) stats_put.n_success, (long unsigned) stats_put.n_failures);
  printf("%.1f %.1f ", lat_avg, lat_sigma_2);
  // printf("%.2f ", stats_put.latency_sum ? (float) stats_put.n_success * 1000.0 / (float) stats_put.latency_sum : 0);

  printf("%.2f ", total_time_ms != 0 ? (float) stats_put.n_success * 1000.0 / total_time_ms : 0);

  printf("\n");
}

void print_stats_get_nolock(void)
{
  double lat_sigma_2 = 0;
  double lat_avg = 0;

  if (stats_get.n_success > 0)
    {
      lat_avg = (double) stats_get.latency_sum / stats_get.n_success;
      lat_sigma_2 = sqrt(stats_get.latency_sq_sum / stats_get.n_success - lat_avg*lat_avg);
    }

  printf("time=%lu ok=%llu fail=%llu bytes=%llu (%lluMbit/s) aver %lluMbit/s latency: %.1f ms sd: %.1f ops: %.2f (g)\n",
          mainstats.tv2.tv_sec - mainstats.tv1.tv_sec,
          (unsigned long long)stats_get.n_success,
          (unsigned long long)stats_get.n_failures,
          (unsigned long long)stats_get.n_bytes,
          (unsigned long long)mainstats.speed/timebase,
          (unsigned long long)mainstats.total_speed/(mainstats.i*timebase),
          lat_avg,
          lat_sigma_2,
          ((float)(mainstats.tv2.tv_sec - mainstats.tv1.tv_sec) * 1000 + (mainstats.tv2.tv_usec - mainstats.tv1.tv_usec)/1000) > 0 ? (float) stats_get.n_success / (((float)(mainstats.tv2.tv_sec - mainstats.tv1.tv_sec) * 1000 + (mainstats.tv2.tv_usec - mainstats.tv1.tv_usec)/1000) / 1000.0) : 0
        );
}

void print_gpstats_get_nolock(void)
{
  double lat_sigma_2 = 0;
  double lat_avg = 0;
  double total_time_ms;

  if (stats_get.n_success > 0)
    {
      lat_avg = (double) stats_get.latency_sum / stats_get.n_success;
      lat_sigma_2 = sqrt(stats_get.latency_sq_sum / stats_get.n_success - lat_avg*lat_avg);
    }

  total_time_ms = (double) (mainstats.tv2.tv_sec - mainstats.tv1.tv_sec) * 1000.0 + (mainstats.tv2.tv_usec - mainstats.tv1.tv_usec) /1000.0;

  /* (d/p/g) nt class size time latency sd ops*/

  printf("# 2 n_threads class max_block_size seed time n_succ n_fail lat lat_sd ops\n");
  printf("2 ");
  printf("%d ", n_threads);
  printf("%d ", class);
  printf("%d ", max_block_size);
  printf("%d ", seed);
  printf("%lu ", mainstats.tv2.tv_sec - mainstats.tv1.tv_sec);
  printf("%lu %lu ", (long unsigned) stats_get.n_success, (long unsigned) stats_get.n_failures);
  printf("%.1f %.1f ", lat_avg, lat_sigma_2);

  printf("%.2f ", total_time_ms != 0 ? (float) stats_get.n_success * 1000.0 / total_time_ms : 0);

  printf("\n");
}


void print_stats_del_nolock(void)
{
  double lat_sigma_2 = 0;
  double lat_avg = 0;

  if (stats_delete.n_success > 0)
    {
      lat_avg = (double) stats_delete.latency_sum / stats_delete.n_success;
      lat_sigma_2 = sqrt(stats_delete.latency_sq_sum / stats_delete.n_success - lat_avg*lat_avg);
    }


  printf("time=%lu ok=%llu fail=%llu (%lluops/s) aver %lluops/s latency: %.1f ms sd: %.1f ops: %.2f (d)\n",
          mainstats.tv2.tv_sec - mainstats.tv1.tv_sec,
          (unsigned long long)stats_delete.n_success,
          (unsigned long long)stats_delete.n_failures,
          (unsigned long long)mainstats.speed/timebase,
          (unsigned long long)mainstats.total_speed/(mainstats.i*timebase),
          lat_avg,
          lat_sigma_2,
          ((float)(mainstats.tv2.tv_sec - mainstats.tv1.tv_sec) * 1000 + (mainstats.tv2.tv_usec - mainstats.tv1.tv_usec)/1000) > 0 ? (float) stats_delete.n_success / (((float)(mainstats.tv2.tv_sec - mainstats.tv1.tv_sec) * 1000 + (mainstats.tv2.tv_usec - mainstats.tv1.tv_usec)/1000) / 1000.0) : 0
         );
}

void print_gpstats_del_nolock(void)
{
  double lat_sigma_2 = 0;
  double lat_avg = 0;
  double total_time_ms;

  if (stats_delete.n_success > 0)
    {
      lat_avg = (double) stats_delete.latency_sum / stats_delete.n_success;
      lat_sigma_2 = sqrt(stats_delete.latency_sq_sum / stats_delete.n_success - lat_avg*lat_avg);
    }

  total_time_ms = (double) (mainstats.tv2.tv_sec - mainstats.tv1.tv_sec) * 1000.0 + (mainstats.tv2.tv_usec - mainstats.tv1.tv_usec) /1000.0;

  /* (d/p/g) nt class size time latency sd ops*/

  printf("# 3 n_threads class max_block_size seed time n_succ n_fail lat lat_sd ops\n");
  printf("3 ");
  printf("%d ", n_threads);
  printf("%d ", class);
  printf("%d ", max_block_size);
  printf("%d ", seed);
  printf("%lu ", mainstats.tv2.tv_sec - mainstats.tv1.tv_sec);
  printf("%lu %lu ", (long unsigned) stats_delete.n_success, (long unsigned) stats_delete.n_failures);
  printf("%.1f %.1f ", lat_avg, lat_sigma_2);
  printf("%.2f ", total_time_ms != 0 ? (float) stats_delete.n_success * 1000.0 / total_time_ms : 0);


  printf("\n");
}

void printstats_lock(void)
{
  pthread_mutex_lock(&stats_lock);
  print_stats_put_nolock();
  print_stats_get_nolock();
  print_stats_del_nolock();

  fflush(stdout);
  pthread_mutex_unlock(&stats_lock);
}

void printgpstats_lock(void)
{
  pthread_mutex_lock(&stats_lock);
  print_gpstats_put_nolock();
  print_gpstats_get_nolock();
  print_gpstats_del_nolock();
  
  fflush(stdout);
  pthread_mutex_unlock(&stats_lock);
}

void *timer_thread_main(void *arg)
{
  uint64_t prev_bytes;
  int dummy;

  pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &dummy);
  pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, &dummy);

  pthread_mutex_lock(&stats_lock);
  memset(&mainstats, 0, sizeof(tmainstats));
  prev_bytes = 0;
  mainstats.i = 1;
  gettimeofday(&mainstats.tv1, NULL);
  pthread_mutex_unlock(&stats_lock);

  while (1)
    {
      sleep(timebase);
      pthread_mutex_lock(&stats_lock);
      mainstats.speed = (((stats_put.n_bytes + stats_get.n_bytes) - prev_bytes)*8)/(1024*1024);
      mainstats.total_speed += mainstats.speed;

      gettimeofday(&mainstats.tv2, NULL);

      if (print_running_stats != 0)
        {
          print_stats_put_nolock();
          print_stats_get_nolock();
          print_stats_del_nolock();
          fflush(stdout);
        }
      
      prev_bytes = stats_put.n_bytes + stats_get.n_bytes;
      mainstats.i++;
      pthread_mutex_unlock(&stats_lock);
    }

  pthread_exit(NULL);
}

void doit()
{
  pthread_t timer_thread;
  int i, ret;

  ret = dpl_init();
  if (0 != ret)
    {
      fprintf(stderr, "dpl_init failed\n");
      exit(1);
    }

  memset(&stats_put, 0, sizeof (tstats));
  memset(&stats_get, 0, sizeof (tstats));
  memset(&stats_delete, 0, sizeof (tstats));

  ret = pthread_attr_init(&g_detached_thread_attr);
  if (0 != ret)
    {
      fprintf(stderr, "pthread_attr_init: %d\n", ret);
      exit(1);
    }
  
  (void)pthread_attr_setdetachstate(&g_detached_thread_attr,
                                    PTHREAD_CREATE_DETACHED);

  ret = pthread_attr_init(&g_joinable_thread_attr);
  if (0 != ret)
    {
      fprintf(stderr, "pthread_attr_init: %d\n", ret);
      exit(1);
    }
  
  (void)pthread_attr_setdetachstate(&g_joinable_thread_attr,
                                    PTHREAD_CREATE_JOINABLE);

  ret = pthread_create(&timer_thread, &g_detached_thread_attr,
                       timer_thread_main, NULL);
  if (0 != ret)
    {
      fprintf(stderr, "pthread_create %d", ret);
      exit(1);
    }

  workers = malloc(n_threads * sizeof (pthread_t));
  if (NULL == workers)
    {
      perror("malloc");
      exit(1);
    }

  for (i = 0;i < n_threads;i++)
    {
      tthreadid *threadid;

      threadid = malloc(sizeof (*threadid));
      if (NULL == threadid)
        {
          perror("malloc");
          exit(1);
        }
      threadid->id = i;

      ret = pthread_create(&workers[i], &g_joinable_thread_attr,
                           test_main, threadid);
      if (0 != ret)
        {
          fprintf(stderr, "pthread_create %d", ret);
          exit(1);
        }
    }

  if (vflag)
    fprintf(stderr, "joining threads\n");

  for (i = 0;i < n_threads;i++)
    {
      void *status;

      if ((ret = pthread_join(workers[i], &status)) != 0)
        {
          fprintf(stderr, "pthread_cancel %d", ret);
          exit(1);
        }
    }

  if (vflag)
    fprintf(stderr, "fini\n");

  if (vflag)
    fprintf(stderr, "canceling timer\n");

  if ((ret = pthread_cancel(timer_thread)) != 0)
    {
      fprintf(stderr, "pthread_cancel %d", ret);
      exit(1);
    }

  if (print_running_stats != 0)
    {
      printstats_lock();
    }
  if (print_gnuplot_stats != 0)
    {
      printgpstats_lock();
    }


  if (print_running_stats != 0)
    {
      fprintf(stderr, "n_success %llu %llu %llu\n",
              (unsigned long long)stats_put.n_success, 
              (unsigned long long)stats_get.n_success, 
              (unsigned long long)stats_delete.n_success);
    }
}

void usage()
{
  fprintf(stderr, "usage: dpl_test_id_CRD [-P print gnuplot stats] [-g chance of get] [-t timebase][-R (random content)][-z (fill buffer with 'z's)][-r (random size)][-o (random oids)] [-S use seed] [-N n_threads][-n n_ops][-v (vflag)][-B bucket] [-C class of service] [-b size] [-U (do not send usermd)]\n");
  exit(1);
}

int main(int argc, char **argv)
{
  char opt;
  u_int trace_level = 0u;

  memset(&drbuffer, 0, sizeof(struct drand48_data));

  while ((opt = getopt(argc, argv, "Pt:Rzrb:N:n:vB:C:S:Ug:p:T:o")) != -1)
    switch (opt)
      {
      case 'o':
        oflag = 1;
        break ;
      case 't':
        timebase = atoi(optarg);
        break ;
      case 'R':
        Rflag = 1;
        break ;
      case 'z':
        zflag = 1;
        break ;
      case 'r':
        rflag = 1;
        break ;
      case 'P':
        print_gnuplot_stats = 1;
        print_running_stats = 0;
        break;
      case 'N':
        n_threads = atoi(optarg);
        break ;
      case 'b':
        max_block_size = atoi(optarg);
        break;
      case 'U':
        send_usermd = 0;
        break;
      case 'n':
        n_ops = atoi(optarg);
        break ;
      case 'g':
        maxreads = atoi(optarg);
        break;
      case 'B':
        bucket = strdup(optarg);
        assert(NULL != bucket);
        break ;
      case 'C':
        class = dpl_storage_class(optarg);
        if (-1 == class)
          {
            fprintf(stderr, "bad storage class\n");
            exit(1);
          }
        break;
      case 'S':
        srand48_r(atoi(optarg), &drbuffer);
        seed = atoi(optarg);
        break;
      case 'v':
        vflag++;
        break ;
      case 'p':
        profile = strdup(optarg);
        assert(NULL != profile);
        break ;
      case 'T':
        trace_level = strtoul(optarg, NULL, 0);
        break ;
      case '?':
      default:
        usage();
      }
  argc -= optind;
  argv += optind;

  if (argc != 0)
    usage();

  ctx = dpl_ctx_new(NULL, profile);
  if (NULL == ctx)
    {
      fprintf(stderr, "error creating droplet ctx\n");
      exit(1);
    }

  ctx->trace_level = trace_level;

  doit();
  
  return 0;
}
