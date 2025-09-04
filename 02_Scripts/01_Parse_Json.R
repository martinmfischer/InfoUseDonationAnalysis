# ------------------------------------------------------------
# Script: parse_all_json_robust.R
# Task  : Robustly load all JSONs in 01_Data, extract params,
#         flag malformed/empty files, save combined df
# ------------------------------------------------------------



pacman::p_load(
  "jsonlite",
  "dplyr",
  "stringr",
  "tidyverse",
  "purrr"
)


master_list <- list() ## the top level list




# files
files <- list.files("01_Data", pattern = "\\.json$", full.names = TRUE)

# extract params from basename

extract_params <- function(fname) {
  bn <- basename(fname)
  
  participant <- str_match(bn, "participant=([^_]+)")[,2]
  
  # key may be empty, so allow 0 or more characters until .json
  key <- str_match(bn, "key=([^.]*)")[,2]
  
  # source may contain underscores, stop at _key= or end
  source <- str_match(bn, "source=([^_]+)")[,2]
  
  list(
    participant = participant,
    key         = key,
    source      = source
  )
}



parse_json_clean <- function(file) {
  
  
  detect_export_type <- function(parsed_json) {
    
    # Example usage:
    # parsed_json <- fromJSON("file.json", simplifyVector = FALSE)
    # detect_export_type(parsed_json)
    # => "facebook" or "whatsapp"
    
    # get top-level names
    top_names <- names(parsed_json)
    
    if (is.null(top_names)) return(NA_character_)
    
    # check for known Facebook fields
    facebook_fields <- c("facebook_comments", 
                         "facebook_likes_and_reactions", 
                         "facebook_followed_pages")
    
    # check for known WhatsApp fields
    whatsapp_fields <- c("whatsapp_chats")
    
    # Facebook: any of the Facebook fields present
    if (any(facebook_fields %in% top_names)) return("facebook")
    
    # WhatsApp: any of the WhatsApp fields present
    if (any(whatsapp_fields %in% top_names)) return("whatsapp")
    
    # Unknown
    return("unknown")
  }
  

  
  params <- extract_params(file)
  
  if (params$source != "Multiple") return(NULL)
  
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
  attr(j, "participant") <- params$participant
  attr(j, "key") <- params$key
  attr(j, "platform") <- detect_export_type(j)
  
  
  return(j)

}

 


 for (f in files) {
  
  parsed <- parse_json_clean(f)
  if(is.null(parsed)) {
    message("Got no data for file ", basename(f))
    message("Params: ", paste(extract_params(f), "|"))
    next
  }
  
  pid <- attr(parsed, "participant")
  platform <- attr(parsed, "platform")
  key <- attr(parsed, "key")
  str(parsed)
  # initialize participant entry if missing
  if (is.null(master_list[[pid]])) master_list[[pid]] <- list()
  
  if(is.na(platform)) next
  # for Facebook: store under "facebook"
  if (platform == "facebook") {
    master_list[[pid]]$facebook <- parsed
  } 
  
  # for WhatsApp: allow multiple chats
  if (platform == "whatsapp") {
    if (is.null(master_list[[pid]]$whatsapp)) master_list[[pid]]$whatsapp <- list()
    # use key or increment to name the chat
    chat_name <- attr(parsed, "key")
    master_list[[pid]]$whatsapp[[chat_name]] <- parsed
  }
  
  #print(parsed)
}

