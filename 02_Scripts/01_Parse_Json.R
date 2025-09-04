# ------------------------------------------------------------
# Script: parse_all_json_robust.R
# Task  : Robustly load all JSONs in 01_Data, extract params,
#         flag malformed/empty files, save combined df
# ------------------------------------------------------------

library(jsonlite)
library(dplyr)
library(stringr)
library(tidyverse)
library(purrr)

`%+%` <- function(e1, e2) {
  if (is.character(e1) && is.character(e2)) {
    paste0(e1, e2)
  } else {
    base::`+`(e1, e2)
  }
}



# files
files <- list.files("01_Data", pattern = "\\.json$", full.names = TRUE)

# extract params from basename
extract_params <- function(fname) {
  bn <- basename(fname)
  participant <- str_match(bn, "participant=([^_]+)")[,2]
  key         <- str_match(bn, "key=([^\\.]+)")[,2]
  list(participant = participant, key = key)
}



parse_json_clean <- function(file) {
  j <- tryCatch(fromJSON(file, simplifyVector = FALSE), error = function(e) return(e))
  if (inherits(j, "error")) {
    cat("WARNING! ERROR WHILE PARSING JSON FILE:")
    cat(file)
    return(j)
  }
  name_vec <- c()
  message("Iterating over ", length(seq_along(j)), " elements in file ", basename(file))
  for (i in seq_along(j)) {
    
    element <- j[[i]]
    element_name <- names(j[[i]])
    name_vec <- c(name_vec, element_name)
    
   
    if (is.list(element) && length(element) == 1) {
      # message("Iterating over element: ", names(element),
      #         " that has class ", class(element))
      
      inner <- element[[1]]

      j[[i]] <- inner
      
    }
  }
  names(j) <- name_vec
  
  return(j)

}

 


 for (f in files) {
  params <- extract_params(f)
  parsed <- parse_json_clean(f)
  #print(parsed)
}

