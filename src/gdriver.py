import sys
import pickle
import os.path
import getopt
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from apiclient.http import MediaFileUpload, MediaIoBaseDownload
from apiclient import errors


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive']


def connect(secret_file):
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                secret_file, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('drive', 'v3', credentials=creds)
    return service


def getlist(service, filespec):
    # Call the Drive v3 API
    results = service.files().list(q=f"name contains '{filespec}' and trashed = false",
                                   fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])

    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            print(f"    {item['name']} ({item['id']})")


def get_file_info(service, filename):
    # Call the Drive v3 API
    results = service.files().list(q=f"name = '{filename}' and trashed = false",
                                   fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])

    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            print(f"{item['name']} ({item['id']})")


def upload(drive_service, filename, mime, folder=None):
    file_metadata = {
        'name': filename
    }
    if folder:
        file_metadata['parents'] = [folder]

    media = MediaFileUpload(f'{filename}', mimetype=mime)
    file = drive_service.files().create(body=file_metadata,
                                        media_body=media,
                                        fields='id').execute()
    print('File ID: %s' % file.get('id'))


def update(service, file_id, filename):
    try:
        # retrieve the file from the API.
        file = service.files().get(fileId=file_id).execute()
        del file['id']  # else service.files().update() will fail

        # setup new content.
        media_body = MediaFileUpload(filename, resumable=True)

        # update content of the file
        updated_file = service.files().update(
            fileId=file_id,
            body=file,
            media_body=media_body).execute()
    except errors.HttpError as error:
        print('An error occurred: %s' % error)
        return None

    print(f"Updated: {filename} ({file_id})")
    return updated_file


def execute(cmd, params):
    try:
        gdrive = connect(params['credential'])
    except FileNotFoundError as err:
        print(f"Connect error: {err}")
        sys.exit(1)

    if cmd == 'update':
        update(gdrive, params['file_id'], params['filename'])


def main():
    params = {
        'credential': 'credentials.json',
    }

    try:
        cmd, argv = sys.argv[1], sys.argv[2:]

        # parse command line options
        opts, args = getopt.getopt(argv, 'k:')
        options = dict(opts)
        if '-k' in options.keys():
            params['credential'] = Options['-k']

        if cmd == 'update':
            file_id, filename = argv
            params['file_id'] = file_id
            params['filename'] = filename
        else:
            print("unknown command")
            sys.exit(1)
    except getopt.GetoptError:
        print('Invalid command line option or arguments')
        sys.exit(2)
    except ValueError:
        print(f"syntax:\n\t{sys.argv[0]} command param1 param2 ...")
        sys.exit(2)

    execute(cmd, params)


if __name__ == '__main__':
    main()
