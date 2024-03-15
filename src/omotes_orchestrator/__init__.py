#  Copyright (c) 2023. Deltares & TNO
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
import logging
from dotenv import load_dotenv

load_dotenv(verbose=True)

import os
from omotes_sdk.internal.common.app_logging import setup_logging, LogLevel

setup_logging(LogLevel.parse(os.environ.get("LOG_LEVEL", "INFO")), "omotes_orchestrator")
setup_logging(LogLevel.parse(os.environ.get("LOG_LEVEL_SQL", "WARNING")), "sqlalchemy.engine")
