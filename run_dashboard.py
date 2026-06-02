import os
import sys
import http.server
import socketserver
import webbrowser
import threading
import time

PORT = 8524 # Custom port for our visualizer dashboard
DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard")

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

def start_server():
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        print(f"Dashboard server started successfully at http://localhost:{PORT}")
        print(f"Serving files from: {DIRECTORY}")
        print("Press Ctrl+C in this terminal window to stop the server.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down server...")
            httpd.shutdown()

def open_browser():
    # Wait 1.5 seconds for the server to spin up, then open browser
    time.sleep(1.5)
    url = f"http://localhost:{PORT}"
    print(f"Opening browser at {url}...")
    webbrowser.open(url)

def main():
    if not os.path.exists(DIRECTORY):
        print(f"Error: Dashboard directory not found at {DIRECTORY}")
        sys.exit(1)
        
    print("==============================================================")
    print("Databricks Medallion Streaming Pipeline Visualizer Launcher")
    print("==============================================================")
    
    # Run server in main thread and browser opener in secondary thread
    threading.Thread(target=open_browser, daemon=True).start()
    start_server()

if __name__ == "__main__":
    main()
