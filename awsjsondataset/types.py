from typing import List, Union, Iterator, Optional, Dict
from pathlib import Path

# Explicit types
JSONDataset = List[Union[Dict, any]]
JSONLocalPath = Union[Path, str]