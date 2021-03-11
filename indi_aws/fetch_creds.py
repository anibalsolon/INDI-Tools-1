# indi_aws/fetch_creds.py
#

# This module contains functions which return sensitive information from
# a csv file, with regards to connection to AWS services.


# Function to return AWS secure environment variables
def return_aws_keys(creds_path):
    """
    Method to return AWS access key id and secret access key using
    credentials found in a local file.

    Parameters
    ----------
    :type creds_path : str
    :param creds_path : (filepath) path to the csv file downloaded
        from AWS; can either be root or user credentials

    Returns
    -------
    aws_access_key_id : string
        string of the AWS access key ID
    aws_secret_access_key : string
        string of the AWS secret access key

    """

    with open(creds_path, 'r') as creds_in:
        # Grab csv rows
        row1 = creds_in.readline()
        row2 = creds_in.readline()

    # Are they root or user keys
    if 'User Name' in row1:
        # And split out for keys
        aws_access_key_id = row2.split(',')[1]
        aws_secret_access_key = row2.split(',')[2]
    elif 'AWSAccessKeyId' in row1:
        # And split out for keys
        aws_access_key_id = row1.split('=')[1]
        aws_secret_access_key = row2.split('=')[1]
    else:
        err_msg = 'Credentials file not recognized, check file is correct'
        raise Exception(err_msg)

    # Strip any carriage return/line feeds
    aws_access_key_id = aws_access_key_id.replace('\r', '').replace('\n', '')
    aws_secret_access_key = aws_secret_access_key.replace(
        '\r', '').replace('\n', '')

    # Return keys
    return aws_access_key_id, aws_secret_access_key


def return_bucket(creds_path, bucket_name):
    """
    Method to a return a bucket object which can be used to interact
    with an AWS S3 bucket using credentials found in a local file.
    Parameters
    ----------
    :type creds_path: str
    :param creds_path : (filepath) path to the csv file with
        'Access Key Id' as the header and the corresponding ASCII text
        for the key underneath; same with the 'Secret Access Key'
        string and ASCII text
    :type bucket_name: str
    :param bucket_name: string corresponding to the name of the bucket
       on S3
    Returns
    -------
    bucket : boto.s3.bucket.Bucket
        a boto s3 Bucket object which is used to interact with files
        in an S3 bucket on AWS
    """

    try:
        import boto3
        from botocore import handlers as botocore_handlers
        from botocore import exceptions as botocore_exceptions
    except ImportError:
        err_msg = 'Boto3 package is not installed - install boto3 and '\
                  'try again.'
        raise Exception(err_msg)

    # Try and get AWS credentials if a creds_path is specified
    if creds_path:
        try:
            aws_access_key_id, aws_secret_access_key = \
                return_aws_keys(creds_path)
        except Exception as exc:
            print(
                'There was a problem extracting the AWS credentials from the '
                ' credentials file provided: {0}'.format(creds_path, exc))
            raise

        print(
            'Connecting to S3 bucket: {0} with credentials from'
            ' {1} ...'.format(bucket_name, creds_path))
        # Better when being used in multi-threading, see:
        # http://boto3.readthedocs.org/en/latest/guide/resources.html#multithreading
        session = boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key)
        s3_resource = session.resource('s3', use_ssl=True)

    # Otherwise, try to connect via policy
    else:
        print('Connecting to AWS: {0}...'.format(bucket_name))
        session = boto3.session.Session()
        s3_resource = session.resource('s3', use_ssl=True)

    bucket = s3_resource.Bucket(bucket_name)

    def tryout():
        try:
            s3_resource.meta.client.head_bucket(Bucket=bucket_name)
        except botocore_exceptions.ClientError as exc:
            error_code = int(exc.response['Error']['Code'])
            if error_code == 403:
                raise
            elif error_code == 404:
                print(
                    'Bucket: {0} does not exist; check spelling and try '
                    'again'.format(bucket_name))
                raise
            else:
                print('Unable to connect to bucket: {0}'.format(bucket_name, exc))

    try:
        tryout()
    except:
        s3_resource.meta.client.meta.events.register(
            'choose-signer.s3.*', botocore_handlers.disable_signing)
        tryout()

    return bucket
