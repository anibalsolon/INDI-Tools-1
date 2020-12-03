# indi_aws/aws_utils.py
#

# Import packages
import hashlib
import os
import sys

from botocore.exceptions import ClientError


# This module contains functions which assist in interacting with AWS
# services, including uploading/downloading data and file checking.
# Class to track percentage of S3 file upload
class ProgressPercentage(object):
    """
    Callable class instance (via __call__ method) that displays
    upload percentage of a file to S3
    """

    def __init__(self, filename):
        """
        Init the percentage tracker with a filename
        """

        # Import packages
        import threading
        import os

        # Initialize data attributes
        self._filename = filename
        if hasattr(filename, 'content_length'):
            self._size = float(filename.content_length)
        elif hasattr(filename, 'size'):
            self._size = float(filename.size)
        else:
            self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        """
        Call to print the current percentage complete of transfer
        :param bytes_amount: amount recently received, will be added
            to running totals in class private variable
        :return: writes to stdout
        """
        # With the lock on, print upload status
        with self._lock:
            self._seen_so_far += bytes_amount
            if self._size != 0:
                percentage = (self._seen_so_far / self._size) * 100
            else:
                percentage = 0
            progress_str = '{0} / {1} ({2:.2f}%)\r'.format(
                self._seen_so_far, self._size, percentage)

            # Write to stdout
            sys.stdout.write(progress_str)
            sys.stdout.flush()


# Get the MD5 sums of files on S3
def md5_sum(bucket, prefix='', filt_str=''):
    """
        Function to get the filenames and MD5 checksums of files stored in
    an S3 bucket and return this as a dictionary.

    Parameters
    ----------
    :type bucket: boto3.bucket
    :param bucket : boto3 Bucket instance
        an instance of the boto3 S3 bucket class to download from
    :type prefix: str
    :param prefix : string (optional), default=''
        the bucket prefix where all of the file keys are located
    :type filt_str: str
    :param filt_str : string (optional), default=''
        a string to filter the filekeys of interest;
        e.g. 'matrix_data' will only return filekeys with the string
        'matrix_data' in their filepath name

    Returns
    -------
    :return: md5_dict : dictionary {str : str}
        a dictionary where the keys are the S3 filename and the values
        are the MD5 checksum values

    """

    # Init variables
    blist = bucket.objects.filter(Prefix=prefix)
    md5_dict = {}

    # And iterate over keys to copy over new ones
    for bkey in blist:
        filename = str(bkey.key)
        if filt_str in filename:
            md5_sum_val = str(bkey.etag).strip('"')
            md5_dict[filename] = md5_sum_val
            print('filename: {0}'.format(filename))
            print('md5_sum: {0}'.format(md5_sum_val))

    # Return the dictionary
    return md5_dict


# Rename s3 keys from src_list to dst_list
def s3_rename(bucket, src_dst_tuple, keep_old=False, make_public=False):
    """
        Function to rename files from an AWS S3 bucket via a copy and delete
    process. Uses all keys in src_list as the original names and renames
    the them to the corresponding keys in the dst_list.
    (e.g. src_list[9] --> dst_list[9])

    Parameters
    ----------
    :param bucket : boto3 Bucket instance
        an instance of the boto3 S3 bucket class to download from
    :param src_dst_tuple : tuple
        a tuple of src and dstlocal lists where src_dst_tuple[0] is the
        src_list and src_dst_tuple[1] is the corresponding dst list;
        src_list[n] would be renamed and saved as dst_list[n] on the S3
        bucket provided
    :param keep_old : boolean (optional), default=False
        flag indicating whether to keep the src_list files
    :param make_public : boolean (optional), default=False
        set to True if files should be publically available on S3

    Returns
    -------
    :return:
    None
        The function doesn't return any value, it deletes data from
        S3 and prints its progress and a 'done' message upon completion

    """

    # Init variables
    src_list = src_dst_tuple[0]
    dst_list = src_dst_tuple[1]

    # Check list lengths are equal
    if len(src_list) != len(dst_list):
        raise ValueError('src_list and dst_list are different lengths!')

    # Init variables
    num_files = len(src_list)

    # Check if the list lengths match
    if num_files != len(dst_list):
        msg = "src_list {0} and dst_list {0} must be the same length".format(
            src_list, dst_list)
        raise RuntimeError(msg)

    # And iterate over keys to copy over new ones
    for idx, src_f in enumerate(src_list):
        src_key = bucket.Object(key=src_f)
        try:
            src_key.get()
        except ClientError:
            print('source file {0} does not exist, skipping... '.format(src_f))
            continue

        # Get corresponding destination file
        dst_key = dst_list[idx]
        dst_obj = bucket.Object(key=dst_key)
        try:
            dst_obj.get()
            print('Destination key {0} exists, skipping ...'.format(dst_key))
            continue
        except ClientError:
            print('copying source: {0} to destination {1}'.format(
                str(src_f), dst_key))

            if make_public:
                print('making public...')
                dst_obj.copy_from(
                    CopySource=bucket.name + '/' + str(src_f),
                    ACL='public-read')
            else:
                dst_obj.copy_from(CopySource=bucket.name + '/' + str(src_f))
            if not keep_old:
                src_key.delete()

        # Print status
        per = 100*(float(idx+1)/num_files)
        print('Done renaming {0}/{1}\n{2:.3f}% complete'.format(
            idx+1, num_files, per))

    # Done iterating through list
    return None


# Delete s3 keys based on input list
def s3_delete(bucket, bucket_keys):
    """
    Method to delete files from an AWS S3 bucket that have the same
    names as those of an input list to a local directory.

    Parameters
    ----------
    :param bucket : boto3 Bucket instance
        an instance of the boto3 S3 bucket class to delete from
    :param bucket_keys : list
        a list of relative paths of the files to delete from the bucket


    Returns
    -------
    :return:
    None
        The function doesn't return any value, it deletes data from
        S3 and prints its progress and a 'done' message upon completion
    """

    # Init variables
    num_files = len(bucket_keys)

    # Iterate over list and delete S3 items
    for idx, bkey in enumerate(bucket_keys):
        try:
            print('attempting to delete {0} from {1}...'.format(
                bkey, bucket.name))
            bobj = bucket.Object(bkey)
            bobj.delete()
            per = 100*(float(idx+1)/num_files)
            print('Done deleting {0}/{1}\n{2:f}% complete'.format(
                idx+1, num_files, per))
        except Exception as exc:
            print('Unable to delete bucket key {0}. Error: {1}'.format(
                bkey, exc))

    # Done iterating through list
    return None


# Download files from AWS S3 to local machine
def s3_download(bucket, s3_local_tuple):
    """
    Function to download files from an AWS S3 bucket that have the same
    names as those of an input list to a local directory.

    Parameters
    ----------
    :param bucket : boto3 Bucket instance
        an instance of the boto3 S3 bucket class to download from
    :param s3_local_tuple : tuple
        a tuple of s3 and local lists where s3_local_tuple[0] is the
        s3_list and s3_local_tuple[1] is the corresponding local list;
        s3_list[n] would be downloaded and saved as local_list[n]

    Returns
    -------
    :return:
    None
        The function doesn't return any value, it downloads data from
        S3 and prints its progress and a 'done' message upon completion
    """

    # Init variables
    s3_list = s3_local_tuple[0]
    local_files = s3_local_tuple[1]
    num_files = len(s3_list)

    # Get file paths from S3 with prefix
    for idx, bkey in enumerate(s3_list):
        # Create a new key from the bucket and set its contents
        bobj = bucket.Object(key=bkey)

        # See if need to upload
        try:
            # If it exists, compare md5sums
            bobj.get()
        except ClientError as exc:
            print("{0} does not exist in S3 bucket! {1}, Skipping ...".format(
                bkey, exc))
            continue

        s3_md5 = bobj.e_tag.strip('"')

        # Get local path
        local_path = local_files[idx]

        # Create subdirs if necessary
        dirname = os.path.dirname(local_path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        # If it exists, check its md5 before skipping
        if os.path.exists(local_path):
            if os.path.isdir(local_path):
                continue
            in_read = open(local_path, 'rb').read()
            local_md5 = hashlib.md5(in_read).hexdigest()
            if local_md5 == s3_md5:
                print('Skipping {0}, already downloaded...'.format(bkey))
            else:
                try:
                    print('Overwriting {0} ...'.format(local_path))
                    bucket.download_file(bkey, local_path,
                                         Callback=ProgressPercentage(bobj))
                except Exception as exc:
                    print(
                        'Could not download file {0} because of: {1}, '
                        'skipping..'.format(bkey, exc))
        else:
            print('Downloading {0} to {1}'.format(bkey, local_path))
            bucket.download_file(bkey, local_path,
                                 Callback=ProgressPercentage(bobj))

        # Print status
        per = 100*(float(idx+1)/num_files)
        print("finished file {0}/{1}\n{2:f}% complete\n".format(
            idx+1, num_files, per))

    # Done iterating through list
    return None


# Upload files to AWS S3
def s3_upload(bucket, local_s3_tuple, make_public=False, encrypt=False):
    """
    Function to upload a list of data to an S3 bucket

    Parameters
    ----------
    :param bucket : boto3 Bucket instance
        an instance of the boto3 S3 bucket class to upload to
    :param local_s3_tuple : tuple
        a tuple of local and s3 lists where local_s3_tuple[0] is the
        local_list and local_s3_tuple[1] is the corresponding s3 list;
        local_list[n] would be uploaded and saved as s3_list[n]
        in the bucket
    :param make_public : boolean (optional), default=False
        set to True if files should be publically read-able on S3
    :param encrypt : boolean (optional), default=False
        set to True if the uploaded files should overwrite what is
        already there

    Returns
    -------
    :return:
    None
        The function doesn't return any value, it uploads data to S3
        and prints its progress and a 'done' message upon completion
    """

    # Init variables
    local_list = local_s3_tuple[0]
    s3_list = local_s3_tuple[1]
    num_files = len(local_list)
    s3_str = 's3://'
    extra_args = {}

    # If make public, pass to extra args
    if make_public:
        extra_args['ACL'] = 'public-read'

    # If encryption is desired init extra_args
    if encrypt:
        extra_args['ServerSideEncryption'] = 'AES256'

    # Check if the list lengths match
    if num_files != len(s3_list):
        raise RuntimeError("local_list and s3_list must be the same length!")

    # For each source file, upload
    for idx, src_file in enumerate(local_list):
        # Get destination path
        dst_file = s3_list[idx]

        # Check for s3_prefix
        if src_file.startswith(s3_str):
            bucket_name = src_file.split('/')[2]
            src_file = src_file.replace(s3_str+bucket_name, '').lstrip('/')
        elif dst_file.startswith(s3_str):
            bucket_name = dst_file.split('/')[2]
            dst_file = dst_file.replace(s3_str+bucket_name, '').lstrip('/')

        # Print status
        print('Uploading {0} to S3 bucket {1} as {2}'.format(
            src_file, bucket.name, dst_file))

        # Create a new key from the bucket and set its contents
        dst_key = bucket.Object(key=dst_file)

        # See if need to upload
        try:
            # If it exists, compare md5sums
            dst_key.get()
            dst_md5 = str(dst_key.e_tag.strip('"'))
            src_read = open(src_file, 'rb').read()
            src_md5 = hashlib.md5(src_read).hexdigest()
            # If md5sums dont match, re-upload via except ClientError
            if src_md5 != dst_md5:
                bucket.upload_file(src_file, dst_file, ExtraArgs=extra_args,
                                   Callback=ProgressPercentage(src_file))
        except ClientError:
            bucket.upload_file(src_file, dst_file, ExtraArgs=extra_args,
                               Callback=ProgressPercentage(src_file))

        per = 100*(float(idx+1)/num_files)
        print("finished file {0}/{1}\n\n{2:f}% complete\n".format(
            idx+1, num_files, per))

    # Print when finished
    return None


# Test write-access to bucket
def test_bucket_access(creds_path, output_directory):
    """
    Function to test write-access to an S3 bucket.

    Parameters
    ----------
    :param creds_path : string
        path to the csv file downloaded from AWS; can either be root
        or user credentials
    :param output_directory : string
        directory to path on S3 where write-access should be tested;
        e.g. 's3://bucket_name/path/to/outputdir'

    Returns
    -------
    :return:
    s3_write_access : boolean
        flag indicating whether user credentials grant write-access to
        specified output directory in S3 bucket
    """

    # Import packages
    import os
    import tempfile

    import botocore.exceptions as bexc
    from indi_aws import fetch_creds

    # Init variables
    s3_str = 's3://'
    test_file = tempfile.mktemp()

    # Explicitly lower-case the "s3"
    if output_directory.lower().startswith(s3_str):
        out_dir_sp = output_directory.split('/')
        out_dir_sp[0] = out_dir_sp[0].lower()
        output_directory = '/'.join(out_dir_sp)

    # Get bucket name
    bucket_name = output_directory.replace(s3_str, '').split('/')[0]

    # Get bucket
    bucket = fetch_creds.return_bucket(creds_path, bucket_name)

    # Create local file
    with open(test_file, 'w') as f:
        f.write('test123')
    f.close()

    # Formulate test ouput key in bucket path output directory
    rel_key_path = output_directory.replace(
        os.path.join(s3_str, bucket_name), '').lstrip('/')
    write_test_key = os.path.join(rel_key_path, os.path.basename(test_file))

    # Attempt a write to bucket
    try:
        bucket.upload_file(test_file, write_test_key)
        print('S3 write access confirmed!')
        test_key = bucket.Object(key=write_test_key)
        test_key.delete()
        s3_write_access = True
    # Otherwise we set the access flag to false
    except bexc.ClientError:
        print('S3 write access is not available!')
        s3_write_access = False

    # Return the access flag
    return s3_write_access
