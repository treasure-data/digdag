#!/usr/bin/env python
import argparse
import logging
import json
import subprocess
import sys
import os.path
import urllib2

from base64 import b64decode
from distutils.dir_util import mkpath
from tempfile import TemporaryFile
from shutil import copyfileobj
from urlparse import urlparse
from urllib2 import urlopen
from StringIO import StringIO

from boto import connect_s3
from boto.kms.layer1 import KMSConnection


def download_to_file(s3, src, f):
    logging.debug('download_to_file: %s -> %s', src, f)
    url = urlparse(src)
    if url.scheme == 's3':
        bucket = s3.get_bucket(url.netloc, validate=False)
        key = bucket.get_key(url.path, validate=False)
        key.get_contents_to_file(f)
    else:
        response = urlopen(src)
        copyfileobj(response, f, 16 * 1024)


def download_to_filename(s3, src, dst, mode=None):
    dirname, os.path.dirname = os.path.split(dst)
    mkpath(dirname)
    with open(dst, 'wb') as f:
        download_to_file(s3, src, f)
    if mode is not None:
        os.chmod(dst, mode)


def download_to_string(s3, src):
    logging.debug('download_to_string: %s', src)
    f = StringIO()
    download_to_file(s3, src, f)
    s = f.getvalue()
    logging.debug('download_to_string: %s: %s', src, s)
    f.close()
    return s


def download(s3, src=None, dst=None, mode=None):
    assert src and dst
    logging.info('download: %s -> %s', src, dst)
    download_to_filename(s3, src, dst, mode=mode)


def process_parameter(kms, parameter):
    logging.debug('process_parameter: %s', parameter)
    t = parameter['type']
    value = parameter['value']
    if t == 'plain':
        return value
    elif t == 'kms_encrypted':
        decrypted = kms.decrypt(value.decode('base64'))
        return decrypted['Plaintext']
    else:
        raise Exception("Unexpected parameter type: '%s'" % (t, ))


def debug_parameter(kms, parameter):
    t = parameter['type']
    if t == 'plain':
        return parameter['value']
    elif t == 'kms_encrypted':
        return '***'
    else:
        return '<unknown:%s>' % (t, )


def process_env(kms, parameters):
    return dict((key, process_parameter(kms, parameter)) for key, parameter in parameters.iteritems())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('config_uri')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('--aws-credentials', type=file)
    args = parser.parse_args()

    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s',
        level=(logging.DEBUG if args.verbose else logging.INFO))

    aws_credentials = {}
    if args.aws_credentials:
        aws_credentials = json.loads(args.aws_credentials.read())

    s3 = connect_s3(host='s3.amazonaws.com', **aws_credentials)

    # Fetch config json
    config_filename = os.path.basename(args.config_uri)
    download_to_filename(s3, args.config_uri, config_filename)
    with open(config_filename, 'rb') as f:
        config = json.load(f)

    # Compile environment variables
    env_parameters = {}
    kms = KMSConnection(**aws_credentials)
    env_parameters = process_env(kms, config.get('env', {}))

    # Create working directory
    working_directory = config['working_directory']
    mkpath(working_directory)

    # Download staging files
    for item in config.get('download', []):
        download(s3, **item)

    # Execute command
    env = dict(os.environ)
    env.update(env_parameters)
    debug_command = [debug_parameter(kms, parameter) for parameter in config['command']]
    command = [process_parameter(kms, parameter) for parameter in config['command']]
    logging.info('executing command: %s', debug_command)
    logging.debug('Popen: command=%s, env=%s', command, env)
    process = subprocess.Popen(command, env=env, cwd=working_directory)
    return process.wait()


if __name__ == '__main__':
    status = main()
    sys.exit(status)
