# webfocusedscrape
Focused scraping component for the Statistical Scraping concept in Official Statistics.

## WEB-FOSS-NL
This repo is part of the WEB-FOSS-NL project on statistical scraping.
More info on statistical scraping [here](https://github.com/SNStatComp/SSIG) 

# Getting started
- Install all required packages using 
    > pip install -r requirements.txt
- Activate the environment
- run the following command to install modules in src as packages for proper import
    > pip install -e .
- Create a `config.yaml` file using `config_template.yaml`
- In the config file specify the input files:
    - `urls`: the filename with the given urls, see also `urls_template.txt`
    - `keywords`: the filename with the target keywords, see also `keywords_template.txt`

# Known bugs and work in progress
- wip: main.py just fetches a few example pages, not yet what the project intends to do
- wip: crawling module is not fully tested, imports not sorted out, test code doesn't yield results
