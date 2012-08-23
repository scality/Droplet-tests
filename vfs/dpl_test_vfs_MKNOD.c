/**
 * @file   dpl_test_vfs_MKNOD.c
 * @author vr <vr@bizanga.com>
 * @date   Wed Mar  4 09:58:56 2009
 * 
 * @brief  put load tests
 * 
 * 
 */

#include <droplet.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>
#include <math.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <fcntl.h>

#include "common.h"

int Rflag = 0; //random content
int zflag = 0; //set 'z' in content
int rflag = 0; //use random size
int vflag = 0; //verbose
int oflag = 0; //random oids
int Hflag = 2; //hash depth
int max_blob_size = 30000;
int n_ops = 1; //per thread
int n_threads = 10;
int timebase = 1; //seconds
char *prefix = "VFS_MKNOD";
dpl_ctx_t *ctx = NULL;

struct drand48_data drbuffer;

pthread_attr_t g_detached_thread_attr, g_joinable_thread_attr;

static pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;

static pthread_t *workers = NULL;

struct
{
  uint64_t n_failures;
  uint64_t n_success;
  uint64_t n_bytes;
  double latency_sq_sum;
  uint64_t latency_sum;
} stats;

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
  int i;
  int ret = 0;
  int dummy;
  char *blob;
  size_t blob_size;
  u64 oid;
  dpl_status_t ret_dpl;
  
  pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &dummy);
  pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, &dummy);

  pthread_cleanup_push(test_cleanup, arg);

  for (i = 0; i < n_ops;i++)
    {
      char *file_name = NULL;
      struct timeval tv1, tv2;
      char path[1024];

      pthread_testcancel();

      oid = dpltest_get_oid(oflag, &drbuffer);

      ret = dpltest_gen_file_name_from_oid(oid, &file_name);
      if (DPL_SUCCESS != ret)
        {
          fprintf(stderr, "unable to generate oid: %s (%d)\n", dpl_status_str(ret), ret);
          exit(1);
        }

      if (rflag)
        blob_size = rand() % max_blob_size;
      else
        blob_size = max_blob_size;
           
      if (vflag)
        fprintf(stderr, "%d: put id %s blob_size %llu\n",
                gettid(), file_name,
                (unsigned long long)blob_size);

      blob = malloc(blob_size);
      if (NULL == blob)
        {
          perror("malloc");
          exit(1);
        }

      if (Rflag)
        dpltest_gen_data(file_name, blob, blob_size);
      else if (zflag)
        memset(blob, 'z', blob_size);
      else
        memset(blob, 0, blob_size);

      gettimeofday(&tv1, NULL);

      ret_dpl = dpltest_path_make(ctx, path, sizeof (path), prefix, file_name, "test", Hflag, 0);
      if (DPL_SUCCESS != ret_dpl)
        {
          fprintf(stderr, "make path for %s failed: %s (%d)\n", file_name, dpl_status_str(ret_dpl), ret_dpl);
          exit(1);
        }

    retry:
      ret_dpl = dpltest_upload_file(ctx, path, blob, blob_size, 0, -1);
      if (DPL_SUCCESS != ret_dpl)
        {
          if (DPL_ENOENT == ret_dpl)
            {
              ret_dpl = dpltest_path_make(ctx, path, sizeof (path), prefix, file_name, "test", Hflag, 1);
              if (DPL_SUCCESS != ret_dpl)
                {
                  fprintf(stderr, "make path for %s failed: %s (%d)\n", file_name, dpl_status_str(ret_dpl), ret_dpl);
                  exit(1);
                }
              goto retry;
            }

          fprintf(stderr, "upload file failed %s: %s (%d)\n", file_name, dpl_status_str(ret_dpl), ret_dpl);
          exit(1);
        }

      gettimeofday(&tv2, NULL);

      pthread_mutex_lock(&stats_lock);
      if (0 == ret)
        {
	  uint64_t latency = (tv2.tv_sec - tv1.tv_sec)*1000 + (tv2.tv_usec - tv1.tv_usec) / 1000;
          stats.n_success++;
          stats.latency_sum += latency;
	  stats.latency_sq_sum += (double) latency*latency;
          stats.n_bytes += blob_size;
        }
      else
        {
          stats.n_failures++;
        }
      pthread_mutex_unlock(&stats_lock);

      free(blob);
      free(file_name);
    }

  if (vflag)
    fprintf(stderr, "%u done\n", threadid->id);

  pthread_cleanup_pop(0);

  pthread_exit(NULL);
}

void *timer_thread_main(void *arg)
{
  uint64_t prev_bytes, speed, total_speed;
  int dummy, i;
  struct timeval tv1, tv2;

  pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &dummy);
  pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, &dummy);

  total_speed = 0;
  prev_bytes = 0;
  i = 1;
  gettimeofday(&tv1, NULL);
  while (1)
    {
      double lat_sigma_2 = 0;
      double lat_avg = 0;

      sleep(timebase);
      pthread_mutex_lock(&stats_lock);
      speed = ((stats.n_bytes-prev_bytes)*8)/(1024*1024);
      total_speed += speed;


      gettimeofday(&tv2, NULL);

      if (stats.n_success > 0) {
	lat_avg = (double) stats.latency_sum / stats.n_success;
	lat_sigma_2 = sqrt(stats.latency_sq_sum / stats.n_success - lat_avg*lat_avg);
      }

      printf("time=%lu ok=%llu fail=%llu bytes=%llu (%lluMbit/s) aver %lluMbit/s latency: %.1f ms sd: %.1f ops: %.2f\n", 
	     tv2.tv_sec - tv1.tv_sec,
	     (unsigned long long)stats.n_success,
	     (unsigned long long)stats.n_failures,
	     (unsigned long long)stats.n_bytes,
	     (unsigned long long)speed/timebase,
	     (unsigned long long)total_speed/(i*timebase),
	     lat_avg,
	     lat_sigma_2,
	     ((float)(tv2.tv_sec - tv1.tv_sec) * 1000 + (tv2.tv_usec - tv1.tv_usec)/1000) > 0 ? (float) stats.n_success / (((float)(tv2.tv_sec - tv1.tv_sec) * 1000 + (tv2.tv_usec - tv1.tv_usec)/1000) / 1000.0) : 0);
      
      fflush(stdout);
      
      prev_bytes = stats.n_bytes;
      pthread_mutex_unlock(&stats_lock);
      i++;
    }

  pthread_exit(NULL);
}

void doit()
{
  pthread_t timer_thread;
  int i, ret;

  memset(&stats, 0, sizeof (stats));

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

  fprintf(stderr, "n_success %llu\n",
          (unsigned long long)stats.n_success);

}

void usage()
{
  fprintf(stderr, "usage: dpl_test_vfs_MKNOD [-t timebase][-R (random content)][-z (fill buffer with 'z's)][-r (random size)][-o (random oids)] [-S use seed] [-N n_threads][-n n_ops][-v (vflag)][-B base_dir] [-b size] [-H dir_hash_depth (default 2)] [-p profile] [-T trace_level]\n");
  exit(1);
}

int main(int argc, char **argv)
{
  char opt;
  char *profile = NULL;
  u64 trace_level = 0u;
  int trace_buffers = 0;

  memset(&drbuffer, 0, sizeof(struct drand48_data));

  while ((opt = getopt(argc, argv, "t:Rzrb:N:n:vB:oC:S:H:p:T:U")) != -1)
    switch (opt)
      {
      case 'H':
        Hflag = atoi(optarg);
        break ;
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
      case 'N':
        n_threads = atoi(optarg);
        break ;
      case 'b':
        max_blob_size = atoi(optarg);
        break;
      case 'n':
        n_ops = atoi(optarg);
        break ;
      case 'B':
        prefix = strdup(optarg);
        assert(NULL != prefix);
        break ;
      case 'S':
	srand48_r(atoi(optarg), &drbuffer);
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
      case 'U':
        trace_buffers = 1;
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
  ctx->trace_buffers = trace_buffers;

  doit();
  
  return 0;
}
