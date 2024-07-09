# NHS Data Collection

This repository contains a collection of Python scripts designed to collect various free NHS data. The scripts use public APIs and web scraping techniques to gather information on different aspects of the NHS.
## Table of Contents

- [Getting Started](#getting-started)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Scripts Overview](#scripts-overview)
- [Contributing](#contributing)
- [License](#license)

## Getting Started

To get a local copy of this project up and running, follow these steps.

### Prerequisites

You will need to have the following software installed on your machine:

- Python 3.7+
- pip (Python package installer)

Additionally, you will need to set up environment variables for some scripts that require API keys or database connection details.

### Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/your-username/nhs-data-collection.git
    cd nhs-data-collection
    ```

2. Create a virtual environment:

    ```bash
    python -m venv venv
    ```

3. Activate the virtual environment:

    - On Windows:

      ```bash
      venv\Scripts\activate
      ```

    - On macOS/Linux:

      ```bash
      source venv/bin/activate
      ```

4. Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

To use the scripts, you will need to configure environment variables for any required API keys or database connection strings. You can set these in your operating system or in a `.env` file in the root of the project.

### Running a Script

Each script can be run individually. For example, to run the script that collects hospital performance data:

```bash
python latest_list_price_downloader.py
