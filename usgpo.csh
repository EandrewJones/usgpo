#!/bin/csh
#SBATCH --time=14-00:00:00
#SBATCH --mem=MaxMemPerNode
#SBATCH --mail-type=ALL
source $HOME/loadR.csh 3.5
echo 'start Rscript usgpo_api_call.R'
Rscript $HOME/usgpo_api_call.R
