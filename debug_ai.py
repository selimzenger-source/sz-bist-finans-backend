import asyncio
import httpx
import os
import sys
from dotenv import load_dotenv

# Path ekle
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from app.config import get_settings

async def test_abacus():
    load_dotenv()
    settings = get_settings()
    key = settings.ABACUS_API_KEY
    print(f"Abacus Key: {key[:10]}...")
    
    url = "https://routellm.abacus.ai/v1/chat/completions"
    payload = {
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "Borsa İstanbul ASUZU hissesi tavan oldu, haber yok. Analitik bir not yaz. 5 kelime."}],
        "temperature": 0.3
    }
    
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.post(
                url,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json=payload
            )
            print(f"Status Code: {resp.status_code}")
            if resp.status_code == 200:
                print("Response:", resp.json()["choices"][0]["message"]["content"])
            else:
                print("Error Details:", resp.text)
        except Exception as e:
            print("Exception:", str(e))

if __name__ == "__main__":
    asyncio.run(test_abacus())
