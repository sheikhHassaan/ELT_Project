import credentials
import subprocess
import time

def wait_for_postgres(host, max_retries=5, waiting_time=5):
    retries = 1
    while retries <= max_retries:
        try:
            result = subprocess.run(['pg_isready', '-h', host], check=True, capture_output=True, text=True) 
            # check=True            -> To make it throw an exception when command goes wrong.
            # capture_output=True   -> To return the output from executing the command in the result.
            # text=True             -> Return the result output in text format instead of bytes.
            if "accepting connections" in result.stdout:
                print(f'→ Connected to the {host}.') 
                return True
        except subprocess.CalledProcessError as e:
            print(f"🔴 Error connecting to Postgres: {e}")
            print(f'Connection to {host} not established yet. Attempt {retries}/{max_retries}. Waiting {waiting_time} more seconds until next ping.')
            retries += 1
            time.sleep(waiting_time)
    print(f"🔴 Maximum retries reached. Exiting ELT process.")
    return False

if not wait_for_postgres(credentials.source_config["host"]):
    exit(1)

if not wait_for_postgres(credentials.destination_config["host"]):
    exit(1)

dump_command = [
    'pg_dump', 
    '-h', credentials.source_config['host'],
    '-U', credentials.source_config['user'],
    '-d', credentials.source_config['db'],
    '-f', 'data_dump.sql', '-w'
]

subprocess_env = dict(PGPASSWORD=credentials.source_config['password'])

subprocess.run(dump_command, env=subprocess_env, check=True)
print(f"→ Source postgres data dumped to data_dump.sql file.")

load_command = [
    'psql',
    '-h', credentials.destination_config["host"],
    '-U', credentials.destination_config["user"],
    '-d', credentials.destination_config["db"],
    '-f', 'data_dump.sql', '-w'
]

subprocess_env = dict(PGPASSWORD=credentials.destination_config["password"])

subprocess.run(load_command, env = subprocess_env, check=True)
print(f"→ Data dumped from data_dump.sql file to destination postgres.")

print("→ ELT script execuuted successfully!")