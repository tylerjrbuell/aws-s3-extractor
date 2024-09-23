import boto3
import os, sys
import json
import subprocess
from tqdm import tqdm
from shutil import make_archive
from datetime import datetime, timezone
from typing import Union
import concurrent.futures

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


def get_sso_cache_file():
    """Get the path to the SSO cache file for a specific profile."""
    sso_cache_dir = './aws-config/sso/cache'
    if not os.path.exists(sso_cache_dir):
        return None

    for filename in os.listdir(sso_cache_dir):
        file_path = os.path.join(sso_cache_dir, filename)
        with open(file_path, "r") as file:
            data = json.load(file)
            # Check if the file contains credentials for the specified profile
            if data.get('startUrl') and data.get('expiresAt'):
                return file_path

    return None


def is_sso_session_valid(cache_file):
    """Check if the SSO session in the cache file is still valid."""
    if not cache_file:
        return False

    with open(cache_file, "r") as file:
        data = json.load(file)
        expiration_str = data.get("expiresAt")

        if expiration_str:
            expiration = datetime.strptime(expiration_str, "%Y-%m-%dT%H:%M:%SZ")
            expiration = expiration.replace(tzinfo=timezone.utc)

            # Compare current time with expiration time
            if datetime.now(timezone.utc) < expiration:
                return True
    return False

def check_profile_exists(profile_name, check_sso=False):
    """Check if the profile is already configured in AWS CLI."""
    try:
        # Use AWS CLI to check if the profile is configured
        result = subprocess.run(
            f"aws configure list --profile {profile_name}", shell=True, capture_output=True, text=True
        )
        
        if(check_sso):
            if result.returncode == 0:
                return True
            else:
                return False
        else:
            return f'profile {profile_name}' in result.stdout
        
    except subprocess.CalledProcessError as e:
        print(f"Error checking profile: {e}")
        return False


def login_with_sso(profile_name):
    """Trigger AWS SSO login using AWS CLI for a specified profile."""
    try:
        # Run the AWS SSO login command
        print(f"Logging in using SSO profile '{profile_name}'...")
        subprocess.run(f"aws sso login --profile {profile_name}", shell=True, check=True)
        print("Successfully logged in via SSO.")
    except subprocess.CalledProcessError as e:
        print(f"SSO login failed: {e}")
        return False
    return True

def configure_iam_profile(profile_name='default'):
    """Trigger AWS IAM configuration using the AWS CLI."""
    if (not os.path.exists('./aws-config/credentials')):
        try:
            subprocess.run(f"aws configure --profile {profile_name}", shell=True, check=True)
            return True
        except Exception as error:
            print(f"Failed to configure IAM profile: {error}")
            return False
    print('\nYou have pre-configured credentials 👇\n')
    os.system('aws configure list')
    if confirm('\nWould you like to logout and re-setup your configuration? [Y/N] -> '):
        os.unlink('./aws-config/credentials')
        return aws_login()
    return True

def configure_sso_profile(profile_name):
    """Trigger AWS SSO configuration using the AWS CLI."""
    try:
        # Run the aws configure sso command interactively to allow user setup
        print(f"Configuring SSO profile '{profile_name}'...")
        subprocess.run(f"aws configure sso --profile {profile_name}", shell=True, check=True)
        print(f"SSO profile '{profile_name}' configured successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to configure SSO profile: {e}")
        return False
    return True

def configure_credentials(login_method='iam', profile_name=None) -> boto3.Session:
    """
    Configure AWS credentials based on login method.
    :param login_method: 'iam' for IAM user, 'sso' for AWS SSO.
    :param profile_name: Optional profile name to use.
    :return: A boto3 session object.
    """
    session = None
    print('\n|-----⚙️  AWS Credentials Configuration⚙️-----|\n')
    if login_method == 'iam':
        if profile_name and check_profile_exists(profile_name):
            session = boto3.Session(profile_name=profile_name)
        else:
            configure_iam_profile(profile_name=profile_name or 'default')
            session = boto3.Session()  # Default profile
        

    elif login_method == 'sso':
        profile_name = profile_name  or input(
            '\nEnter the name of the SSO profile name you would like to use (ex: my-profile) -> '
        )
        print('Checking for valid SSO session...')
        cache_file = get_sso_cache_file()
        if(check_profile_exists(profile_name, check_sso=True)):
            print("SSO session is still valid for this profile. Using cached credentials.")
            session = boto3.Session(profile_name=profile_name)
        elif check_profile_exists(profile_name):
            print('SSO session is not valid. Re-logging in...')
            # Trigger SSO login using AWS CLI
            if login_with_sso(profile_name):
                session = boto3.Session(profile_name=profile_name)
            else:
                print(f"Failed to login with SSO profile: {profile_name}")
        else:
            print(f"SSO profile '{profile_name}' is not configured.")
            configure_sso_profile(profile_name)
            if(check_profile_exists(profile_name)):
                session = boto3.Session(profile_name=profile_name)
    else:
        print("Invalid login method specified.")

    if session:
        # Test session by calling STS to get the caller identity
        try:
            get_aws_user(profile_name=profile_name if profile_name else 'default')
            print(f"\nUsing {login_method.upper()} profile: {profile_name or 'default'}")
        except Exception as e:
            print('\n[ERROR] Failed to authenticate using the stored credentials, re-login and try again')
            return None

    return session


def get_aws_user(profile_name=None) -> dict:
    """Get the AWS user details for the specified profile."""
    user = os.popen('aws sts get-caller-identity --profile ' + profile_name if profile_name else 'default').read()
    user = json.loads(user)
    print(
        f'\nLogged in as: {user.get("Arn")} \nToken Tags: {", ".join([tag.get("Value", "") for tag in user.get("Tags", [])])}')
    return user

def aws_login() -> Union[boto3.Session, None]:
    """Prompts user for aws credentials and default config
    via the aws cli 

    Returns:
        boto3 session object or None
    """
    sso_user = confirm('\nAre you logging in with an SSO user? [Y/N] -> ')
    if(sso_user):
        return configure_credentials(login_method='sso')
    else:
        return configure_credentials()


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
        print(f'\nFound {total_objects} object(s) in s3://{bucket_name}/{folder_name}')
        max_workers = input(
            f'\nEnter the maximum number of concurrent threads to use for downloads\n[WARNING: May be CPU intensive] (default: {100}) -> '
        ) or 100
        print(
            f'\nExtracting {total_objects} object(s) from s3://{bucket_name}/{folder_name}...\n')
        with tqdm(total=total_objects, ncols=100, desc="Download Progress") as pbar:
            futures = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=int(max_workers)) as executor:
                for obj in objects:
                    futures.append(executor.submit(bucket.download_file, obj.key, obj.key.split('/')[-1]))
                    pbar.update(0.5)
                for future in concurrent.futures.as_completed(futures):
                    pbar.update(0.5)
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
    """Retrieves a s3 URI address from the user and 
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

def main():
    global S3
    print("\n\n|------🪣  S3 Bucket Extractor🪣------|\n")
    if(not S3):
        session = aws_login()
        if (session):
            S3 = session.resource('s3')
        else:
            main()
    try:
        while True:
            bucket_name, folder_name = get_s3_target()
            extract_bucket_contents(bucket_name, folder_name)
    except Exception as error:
        print(f'\n** Error extracting bucket contents: {error} **')
        main()



if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\n\nExiting S3 Bucket Extractor...')
        sys.exit(0)