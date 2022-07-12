<h1 align="center">
UFS Data Tracking Bot: 
</h1>

<h2 align="center">    
UFS Data Transferring Automation to Cloud Data Storage
</h2>

<p align="center">
    <img src="images/tracker_sample_results_img.png" width="1100" height="300">
    <img src="images/uploader_transfer_img.png" width="1000" height="500">
    <img src="images/onprem_datasets_avail_img.png" width="900" height="150">
    <img src="images/cloud_data_bucket_img.png" width="350" height="400">
</p>

<h5 align="center">
    
[Prerequisites](#Prerequisites) • [Dataset](#Dataset) • [Quick Start](#Quick-Start)  • [Environment Setup](#Environment-Setup) • [Status](#Status)
 • [What's Included](#What's-Included) • [Documentation](#Documentation) • [References](#Reference(s))

</h5>

# About

__Introduction:__

Currently, the NOAA development teams' code managers are maintaining their datasets manually via regularly checking if a UFS timestamp dataset is being revised, committed and pushed to the UFS-WM development branch repository to maintain datasets that will only support the latest two-months of development UFS-WM code. There are times when UFS timestamp datasets are unused as they exceed the latest two-months of development UFS-WM code window and are left on-prem. While the EPIC team continues to work in parallel with the NOAA development teams' devleopment in UFS-WM code, the UFS Data Tracking Bot will be able to support the transferring of the UFS-WM's datasets to Cloud as revision are continuously made against the UFS timestamp datasets by developers. The UFS Data Tracking Bot will be able to automatically upload the newly pushed UFS-WM datasets to the cloud bucket reserved for the UFS-WM framework (Refer to https://noaa-ufs-regtests-pds.s3.amazonaws.com/index.html).

__Purpose:__

The purpose of this bot is to detect, track, populate, & transfer the revisions of the timestamp datasets made to the UFS-WM developemnt branch to the cloud bucket reserved for the UFS-WM framework. The data tracking bot will be integrated with Jenkins and will later be integrated with another script which will perform the 2 months window shift of datasets to maintain the NOAA development teams' code managers current practice stored and fulfill the stored data requirements.

The purpose of this program is to transfer the input and baseline datasets residing within the RDHPCS to cloud data storage via chaining API calls to communicate with cloud data storage buckets. The program will support the data required for the current UFS-WM deployed within the RDHPCS as well as support the NOAA development team's data management in maintaining only the datasets committed within the latest N months of their UFS development code (once the program is integrated into Jenkins).

According to Amazon AWS, the following conditions need to be considered when transferring data to cloud data storage:

* Largest object that can be uploaded in a single PUT is 5 GB.
* Individual Amazon S3 objects can range in size from a minimum of 0 bytes to a maximum of 5 TB.
* For objects larger than 100 MB, Amazon recommends using the Multipart Upload capability.
* The total volume of data in a cloud data storage bucket are unlimited.

Tools which could be be utilized to perform data transferring & partitioning (Multipart Upload/Download) are:

* AWS SDK
* AWS CLI
* AWS S3 REST API

All of the AWS provided tools are built on Boto3.

In this demonstration, the framework will implement Python AWS SDK for transferring the tracked UFS datasets from the RDHPCS, Orion, to the cloud data storage with low latency.

The AWS SDK will be implemented for the following reasons:
* To integrate with other python scripts.
* AWS SDK carries addition capabilities/features for data manipulation & transferring compare to the aforementioned alternate tools.

__Capabilities:__

This script will be able to perform the following actions:
* Extract single file daily & parse
* Client makes a direct request for rt.sh from GitHub
* rt.sh is read, preprocessed & extracts the timestamps of the relevant UFS datasets which has been pushed on GitHub.
* Generates a file containing the datasets' timestamps
* Program will compare the last log file with the most recent file containing the datasets' timestamps.

This bot will be able to perform the following actions:
* Multi-threading & partitioning to the datasets to assist in the optimization in uploading performance of the datasets from on-prem to cloud as it tracks the PR'd timestamped dataset updates pushed by developers & approved by code manager(s) of the UFS-WM.


__Future Capabilities:__
Will be integrated with another script, which will perform the 2 months window shift of datasets to maintain the NOAA development teams' code managers current practice and fulfill the stored data requirements.

# Table of Contents
* [Prerequisites](#Prerequisites)
* [Dataset](#Dataset)
* [Quick Start](#Quick-Start)
* [Environment Setup](#Environment-Setup) 
* [Status](#Status)
* [What's Included](#What's-Included)
* [Documentation](#Documentation)
* [References](#Reference(s))

# Prerequisites
* Python 3.9
* Setting up AWS CLI configurations for uploading to Cloud.
* Setting up conda environment w/in RDHPCS.
    * Refer to [Environment Setup](#Environment-Setup)

# Dataset
* On-prem Orion

# Quick Start
* For demonstration purposes, refer to 'rt_revision_tracker_scripts_demo.ipynb'

# Environment Setup:
Install miniconda on your machine. Note: Miniconda is a smaller version of Anaconda that only includes conda along with a small set of necessary and useful packages. With Miniconda, you can install only what you need, without all the extra packages that Anaconda comes packaged with:
Download latest Miniconda (e.g. 3.9 version):

wget https://repo.anaconda.com/miniconda/Miniconda3-py39_4.9.2-Linux-x86_64.sh
Check integrity downloaded file with SHA-256:

sha256sum Miniconda3-py39_4.9.2-Linux-x86_64.sh
Reference SHA256 hash in following link: https://docs.conda.io/en/latest/miniconda.html

## Install Miniconda in Linux:

bash Miniconda3-py39_4.9.2-Linux-x86_64.sh
Next, Miniconda installer will prompt where do you want to install Miniconda. Press ENTER to accept the default install location i.e. your $HOME directory. If you don't want to install in the default location, press CTRL+C to cancel the installation or mention an alternate installation directory. If you've chosen the default location, the installer will display “PREFIX=/var/home//miniconda3” and continue the installation.

For installation to take into effect, run the following command:

source ~/.bashrc
Next, you will see the prefix (base) in front of your terminal/shell prompt. Indicating the conda's base environment is activated.

Once you have conda installed on your machine, perform the following to create a conda environment:
To create a new environment (if a YAML file is not provided)

conda create -n [Name of your conda environment you wish to create]
(OR)

To ensure you are running Python 3.9:

conda create -n myenv Python=3.9
(OR)

To create a new environment from an existing YAML file (if a YAML file is provided):

conda env create -f environment.yml
*Note: A .yml file is a text file that contains a list of dependencies, which channels a list for installing dependencies for the given conda environment. For the code to utilize the dependencies, you will need to be in the directory where the environment.yml file lives.

## Activate the new environment via:
conda activate [Name of your conda environment you wish to activate]
Verify that the new environment was installed correctly via:
conda info --env
*Note:

From this point on, must activate conda environment prior to .py script(s) or jupyter notebooks execution using the following command: conda activate
To deactivate a conda environment:
conda deactivate

## Link Home Directory to Dataset Location on RDHPCS Platform
Unfortunately, there is no way to navigate to the /work/ filesystem from within the Jupyter interface. The best way to workaround is to create a symbolic link in your home folder that will take you to the /work/ filesystem. Run the following command from a linux terminal on Orion to create the link:

ln -s /work /home/[Your user account name]/work
Now, when you navigate to the /home/[Your user account name]/work directory in Jupyter, it will take you to the /work folder. Allowing you to obtain any data residing within the /work filesystem that you have permission to access from Jupyter. This same procedure will work for any filesystem available from the root directory.

*Note: On Orion, user must sym link from their home directory to the main directory containing the datasets of interest.

Open & Run Data Analytics Tool on Jupyter Notebook
Open OnDemand has a built-in file explorer and file transfer application available directly from its dashboard via ...
Login to https://orion-ood.hpc.msstate.edu/
In the Open OnDemand Interface, select Interactive Apps > Jupyter Notbook
Set the following configurations to run Jupyter:
Additonal Information
To create a .yml file, execute the following commands:

## Activate the environment to export:

conda activate myenv
Export your active environment to a new file:

conda env export > [ENVIRONMENT FILENAME].yml

# Status
[![Version badge](https://img.shields.io/badge/Python-3.9-blue.svg)](https://shields.io/)
[![Build badge](https://img.shields.io/badge/Build--gray.svg)](https://shields.io/)

# What's Included
Within the download, you will find the following directories and files:
  
* Tracker Scripts:
    > rt_revision_tracker.py
    > rt_tracker_populate.py
    > rt_tracker_reset.py

* Tracker Demo:
    > rt_revision_tracker_scripts_demo.ipynb
    
* Transfer Scripts:
    > upload_data.py
        * Uploader via AWS SDK
    > transfer_specific_data.py 
        * Executable script for specific dataset to transfer to cloud
    > transfer_bot_data.py  
        * Executable script for datasets recorded by UFS data tracker bot to transfer to cloud
    > get_timestamp_data.py
        * Dataset reader from UFS data's source
    > progress_bar.py
        * Monitors uploading progress of datasets to cloud  
        
* Transfer Demo:
    > data_xfer2cloud_scripts_demo.ipynb
    
* List of Dependencies: 
    > git_env.yml (For Tracker Scripts)
    > cloud_xfer_env.yml (For Transfer Scripts)

# Documentation
* Refer to rt_revision_tracker_scripts_demo.ipynb
* Refer to data_xfer2cloud_scripts_demo.ipynb

# References
* N/A

# Version:
* Draft as of 07/12/22
