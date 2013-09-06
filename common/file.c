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
#include <droplet/vfs.h>
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
  int retries = 0;
  dpl_sysmd_t sysmd;

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

  //XXX buffered and block_size ignored for now

  ret2 = dpl_fput(ctx, path, NULL, NULL, NULL, metadata, &sysmd, blob_buf, blob_size);
  if (DPL_SUCCESS != ret2)
    {
      if (DPL_ENOENT == ret2)
        {
          ret = DPL_ENOENT;
        }
      
      goto retry;
    }
  
  ret = DPL_SUCCESS;

 end:

  if (NULL != metadata)
    dpl_dict_free(metadata);

  return ret;
}
