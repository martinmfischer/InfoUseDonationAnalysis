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
  "purrr",
  "progress"
)

master_list <- list()  # top-level container
files <- list.files("01_Data", pattern = "\\.json$", full.names = TRUE)

extract_params <- function(fname) {
  bn <- basename(fname)
  
  participant <- str_match(bn, "participant=([^_]+)")[,2]
  key <- str_match(bn, "key=([^.]*)")[,2]
  source <- str_match(bn, "source=([^_]+)")[,2]
  
  list(participant = participant, key = key, source = source)
}

parse_json_clean <- function(file) {
  
  detect_export_type <- function(parsed_json) {
    top_names <- names(parsed_json)
    if (is.null(top_names))    {
      return(NA_character_)
    }
    
    facebook_fields <- c("facebook_comments", "facebook_likes_and_reactions", "facebook_followed_pages")
    whatsapp_fields <- c("whatsapp_links_with_context")
    
    if (any(facebook_fields %in% top_names)) return("facebook")
    if (any(whatsapp_fields %in% top_names)) return("whatsapp")
    return("unknown")
  }
  
  params <- extract_params(file)
  if (params$source != "Multiple") return(NULL)
  
  j <- tryCatch(fromJSON(file, simplifyVector = FALSE), error = function(e) return(e))
  if (inherits(j, "error")) {
    message("⚠️ ERROR while parsing file: ", basename(file))
    return(j)
  }
  
  name_vec <- c()
  for (i in seq_along(j)) {
    element <- j[[i]]
    element_name <- names(j[[i]])
    name_vec <- c(name_vec, element_name)
    
    if (is.list(element) && length(element) == 1) {
      j[[i]] <- element[[1]]  # unwrap single-element lists
    }
  }
  names(j) <- name_vec
  attr(j, "participant") <- params$participant
  attr(j, "key") <- params$key
  attr(j, "platform") <- detect_export_type(j)
  
  return(j)
}

# ---------------- Progress bar setup ----------------
pb <- progress_bar$new(
  total = length(files),
  format = "Parsing [:bar] :current/:total (:percent) | Last file: :file",
  clear = FALSE, width = 80
)

# Counters for summary
file_count <- 0
participant_set <- c()
facebook_count <- 0
whatsapp_count <- 0
error_count <- 0
missing_count <- 0

# ---------------- Main loop ----------------
for (f in files) {
  parsed <- parse_json_clean(f)
  
  pb$tick(tokens = list(file = basename(f)))  # update progress bar
  file_count <- file_count + 1
  
  if (is.null(parsed)) {
    message("⚠️ No data in file: ", basename(f))
    missing_count <- missing_count + 1
    next
  }
  if (is.na(attr(parsed, "platform"))) {
    message("⚠️ Malformed JSON file: ", basename(f))
    missing_count <- missing_count + 1
    next
    
  }
  
  if (inherits(parsed, "error")) {
    error_count <- error_count + 1
    next
  }
  
  pid <- attr(parsed, "participant")
  platform <- attr(parsed, "platform")
  key <- attr(parsed, "key")
  
  participant_set <- unique(c(participant_set, pid))
  
  if (is.null(master_list[[pid]])) master_list[[pid]] <- list()
  
  if (is.na(platform)) next
  if (platform == "facebook") {
    master_list[[pid]]$facebook <- parsed
    facebook_count <- facebook_count + 1
  } 
  else if (platform == "whatsapp") {
    if (is.null(master_list[[pid]]$whatsapp)) master_list[[pid]]$whatsapp <- list()
    chat_name <- ifelse(nzchar(key), key, paste0("chat_", length(master_list[[pid]]$whatsapp)+1))
    master_list[[pid]]$whatsapp[[chat_name]] <- parsed
    whatsapp_count <- whatsapp_count + 1
  } else {
    
    missing_count <- missing_count + 1
    
  }
}

# ---------------- Save ----------------
saveRDS(master_list, file = "01_Data/parsed_data.rds")

# ---------------- Summary ----------------
message("\n✅ Parsing finished!")
message("Files processed: ", file_count)
message("Unique participants: ", length(participant_set))
message("Facebook exports loaded: ", facebook_count)
message("WhatsApp exports loaded: ", whatsapp_count)
message("Files with no usable data: ", missing_count)
message("Files with parse errors: ", error_count)
