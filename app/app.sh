#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz


# Collect data
bash prepare_data.sh


# Run the indexer
chmod +x index.sh
bash index.sh

# Run the ranker
chmod +x search.sh
bash search.sh "live album by pianist Les McCann"
bash search.sh "Czech film directed by Karel Zeman"
bash search.sh "1947 American film noir"

exec bash