/**
 * @file   std/src/path.c
 * @author vr <vr@bizanga.com>
 * @date   Mon May 31 11:17:13 2010
 * 
 * @brief  file mgmt routines
 * 
 * 
 */

#include <droplet.h>
#include <sys/param.h>
#include "common.h"

dpl_status_t
dpltest_upload_file(dpl_ctx_t *ctx,
                    char *path,
                    char *blob_buf,
                    int blob_size,
                    int buffered,
                    int block_size)
{
  dpl_status_t ret, ret2;
  dpl_canned_acl_t canned_acl = DPL_CANNED_ACL_PRIVATE;
  dpl_dict_t *metadata = NULL;
  dpl_vfile_t *vfile = NULL;
  int retries = 0;
  dpl_vfile_flag_t flags = 0u;
  dpl_sysmd_t sysmd;
  int remain, buf_size, off;

  memset(&sysmd, 0, sizeof (sysmd));
  sysmd.mask = DPL_SYSMD_MASK_CANNED_ACL;
  sysmd.canned_acl = canned_acl;

 retry:

  if (retries >= 3)
    {
      fprintf(stderr, "too many retries: %s (%d)\n", dpl_status_str(ret), ret);
      ret = DPL_FAILURE;
      goto end;
    }

  retries++;

  if (!buffered)
    {
      ret2 = dpl_put(ctx, ctx->cur_bucket, path, NULL, DPL_FTYPE_REG, metadata, &sysmd, blob_buf, blob_size);
      if (DPL_SUCCESS != ret2)
        {
          if (DPL_ENOENT == ret2)
            {
              ret = DPL_ENOENT;
              goto end;
            }

          goto retry;
        }
    }
  else
    {
      flags = DPL_VFILE_FLAG_CREAT;
      //flags |= DPL_VFILE_FLAG_POST;
      
      ret2 = dpl_openwrite(ctx, path, DPL_FTYPE_REG, flags, metadata, &sysmd, blob_size, NULL, &vfile);
      if (DPL_SUCCESS != ret2)
        {
          if (DPL_ENOENT == ret2)
            {
              ret = DPL_ENOENT;
            }

          goto retry;
        }
      
      remain = blob_size;
      off = 0;
      while (1)
        {
          buf_size = MIN(remain, block_size);

          ret = dpl_write(vfile, blob_buf + off, buf_size);
          if (DPL_SUCCESS != ret)
            {
              fprintf(stderr, "write failed\n");
              goto retry;
            }

          off += buf_size;
        }
      
      ret = dpl_close(vfile);
      if (DPL_SUCCESS != ret)
        {
          fprintf(stderr, "close failed %s (%d)\n", dpl_status_str(ret), ret);
          goto retry;
        }
      
      vfile = NULL;
    }

  ret = DPL_SUCCESS;

 end:

  if (NULL != metadata)
    dpl_dict_free(metadata);

  if (NULL != vfile)
    dpl_close(vfile);

  return ret;
}
