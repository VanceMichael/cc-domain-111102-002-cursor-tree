from pathlib import Path
from app.wire import Replay
from app.directory import read_page
print(read_page(Replay("fixtures/responses.json"), "https://directory.invalid/root"))
