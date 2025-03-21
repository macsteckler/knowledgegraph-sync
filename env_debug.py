from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Print all environment variables
print("\nAll environment variables:")
for key, value in os.environ.items():
    if key.startswith('NEO4J'):
        # Hide password in output
        if 'PASSWORD' in key:
            print(f"{key}: ****")
        else:
            print(f"{key}: {value}") 