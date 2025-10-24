# ------------------------------
# Analysis Script for facebook
# ------------------------------


###Todo - retain DBÖS ID in final link list! Order by that as well!

#### Clean Env
rm(list = ls())
gc()

#### Load/Install Packages
if(!require("pacman", character.only=TRUE)) install.packages("pacman")
pacman::p_load(
  "tidyverse",
  "purrr",
  "dplyr",
  "readxl",
  "stringr",
  "fuzzyjoin",
  "urltools",
  "tidyr",
  "progress"
)




