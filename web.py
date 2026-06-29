import uvicorn
from src.webapi import app

if __name__ == "__main__":
    print("NEbena Web UI → http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
