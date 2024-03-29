import boto3
import os, sys
import json
from tqdm import tqdm
from shutil import make_archive

# init s3 resource
S3 = None

# set output directory
OUTPUT_DIR = 'Extractions'
# set base directory
BASE_DIR = os.path.join(os.path.abspath(os.curdir), OUTPUT_DIR)


def confirm(prompt):
    answer = ""
    while answer not in ["y", "n"]:
        answer = input(prompt).lower()
    return answer == "y"


def get_aws_user():
    user = os.popen('aws iam get-user 2>&1').read()
    try:
        user = json.loads(user)['User']
        print(
            f'\nLogged in as: {user.get("UserName")} \nToken Tags: {", ".join([tag.get("Value") for tag in user.get("Tags")])}')
        return user
    except Exception:
        print('\n[ERROR] Failed to authenticate using the stored credentials, re-login and try again')
        return {}

def aws_login():
    """Prompts user for aws credentials and default config
    via the aws cli 

    Returns:
        boolean: status of login
    """
    config_attempts = 0
    if (not os.path.exists('./aws-config/credentials')):
        while not os.path.exists('./aws-config/credentials') and config_attempts < 3:
            print('\n|-----⚙️  AWS Configuration⚙️-----|\n')
            try:
                config_attempts += 1
                status = os.system('aws configure')
                if (status == 0):
                    return True
            except Exception as error:
                print(error)
                return False
        print('[ERROR] Failed to properly store credentials after several attemps, exiting...')
        sys.exit()
    print('\nYou have pre-configured credentials 👇\n')
    os.system('aws configure list')
    if confirm('\nWould you like to logout and re-setup your configuration? [Y/N] -> '):
        os.unlink('./aws-config/credentials')
        if (aws_login()):
            return True
    return True


def extract_bucket_contents(bucket_name, folder_name):
    """Extracts the contents of a given AWS s3 bucket

    Args:
        bucket_name (string): Name of the bucket
        folder_name (string): Subfolder name within the bucket
    """
    bucket = S3.Bucket(bucket_name)
    objects = bucket.objects.filter(Prefix=folder_name)
    total_objects = sum(1 for _ in objects)
    if total_objects > 0:
        os.makedirs(f'{BASE_DIR}/{bucket_name}/{folder_name}', exist_ok=True)
        os.chdir(f'{BASE_DIR}/{bucket_name}/{folder_name}')
        print(
            f'\nExtracting {total_objects} object(s) from s3://{bucket_name}/{folder_name}...\n')
        with tqdm(total=total_objects, ncols=100, desc="Download Progress") as pbar:
            for obj in objects:
                pbar.update(1)
                path, filename = os.path.split(obj.key)
                if filename:
                    if not os.path.exists(filename):
                        bucket.download_file(obj.key, filename)
        print('\nExtraction completed successfully! 🥳')
        os.chdir(BASE_DIR)
        if confirm("\nWould you also like to compress the bucket contents to a zip file? [Y/N] -> "):
            print(
                f'\nWriting zip file {os.path.join(BASE_DIR,bucket_name)}.zip...')
            make_archive(bucket_name, 'zip',
                         os.path.join(os.curdir, bucket_name))
    else:
        print(
            f'\nNo objects found at the given location: s3://{bucket_name}/{folder_name}')


def get_s3_target():
    """Retrives a s3 URI address from the user and 
    parses it to return the associated bucket name and folder/prefix path

    Returns:
        tuple: bucket name | folder path
    """
    s3_uri = input('\nEnter s3 URI (ex: s3://bucket_name/subfolder) -> ')
    while s3_uri == '':
        print('\n**s3 URI is required!**')
        s3_uri = input('\nEnter s3 URI (ex: s3://bucket_name/subfolder) -> ')
    try:
        bucket_name, folder_name = s3_uri.replace("s3://", "").split("/", 1)
        return bucket_name, folder_name.strip('*')
    except ValueError:
        print('\n** Error parsing s3 URI please try again **')

def aws_auth():
    # Authenticate
    cli_authenticated = aws_login()
    user = get_aws_user()
    return cli_authenticated,user

def main():
    global S3
    cli_authenticated,user = aws_auth()
    if (cli_authenticated and user.get('UserName')):
        S3 = boto3.resource('s3')
    else:
        main()
    try:
        print("\n\n|------🪣  S3 Bucket Extractor🪣------|\n")
        while True:
            bucket_name, folder_name = get_s3_target()
            extract_bucket_contents(bucket_name, folder_name)
    except Exception as error:
        print(f'\n** Error extracting bucket contents: {error} **')
        main()



if __name__ == '__main__':
    main()