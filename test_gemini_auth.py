import os
import json
from google import genai
from google.oauth2.credentials import Credentials
import re
from dotenv import load_dotenv

# Load .env file
load_dotenv("backend/.env")

def test_gemini():
    print("--- Testing Gemini API (google-genai) ---")
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("GEMINI_API_KEY not found in environment")
        return
    
    try:
        client = genai.Client(api_key=api_key)
        # Try gemini-2.0-flash (stable name)
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents="Say 'Hello, I am working! where are you living?'",
        )
        print(f"Gemini Response: {response.text}")
    except Exception as e:
        print(f"Gemini API Error: {e}")

if __name__ == "__main__":
    test_gemini()
