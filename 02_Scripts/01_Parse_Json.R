# ------------------------------------------------------------
# Script: parse_all_json_robust.R
# Task  : Robustly load all JSONs in 01_Data, extract params,
#         flag malformed/empty files, save combined data
# ------------------------------------------------------------

#### Clean Environment ####
rm(list = ls())
gc()
if (!require("pacman", character.only = TRUE)) install.packages("pacman")

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
  
  
  
  if (is.null(master_list[[pid]])) master_list[[pid]] <- list()
  
  for (platform_flag in c("has_whatsapp", "has_facebook")) {
    if (is.null(attr(master_list[[pid]], platform_flag))) {
      attr(master_list[[pid]], platform_flag) <- FALSE
    }
  }

  
  if (is.na(platform)) next
  
  #attributes(parsed) <- NULL #reset lower level attributes to remove double structures
  for(attribute in c("key", "participant", "source", "platform")) {
    attr(parsed, attribute) <- NULL
    
  }
  
  if (platform == "facebook") {
    
    
    master_list[[pid]]$facebook <- parsed
    facebook_count <- facebook_count + 1
    attr(master_list[[pid]], "participant") <- pid
    attr(master_list[[pid]], "has_facebook") <- TRUE
  } 
  else if (platform == "whatsapp") {
    if (is.null(master_list[[pid]]$whatsapp)) master_list[[pid]]$whatsapp <- list()
    chat_name <- ifelse(nzchar(key), key, paste0("chat_", length(master_list[[pid]]$whatsapp)+1))
    master_list[[pid]]$whatsapp[[chat_name]] <- parsed
    whatsapp_count <- whatsapp_count + 1
    attr(master_list[[pid]], "has_whatsapp") <- TRUE
    
  } else {
    
    missing_count <- missing_count + 1
    
  }
  attr(master_list[[pid]], "participant") <- pid
  #message("entry has the following platform attribute: ", attr(master_list[[pid]], "platform"))
}




#### Create nested dataframes from the json list export

safe_field <- function(x, name) {
  v <- x[[name]]
  if (is.null(v)) NA_character_ else as.character(v)
}

messages_to_tbl <- function(msgs) {
  if (is.null(msgs) || length(msgs) == 0) {
    return(tibble(date = character(), link = character(), domain = character()))
  }
  map_dfr(msgs, ~ tibble(
    date   = safe_field(.x, "date"),
    link   = safe_field(.x, "link"),
    domain = safe_field(.x, "domain")
  ))
}

# Erzeuge das nested tibble (jede messages-Zelle ist jetzt ein tibble)
whatsapp_tbl <- imap_dfr(master_list, function(pdata, pid) {
  if (is.null(pdata$whatsapp)) return(NULL)
  imap_dfr(pdata$whatsapp, function(chat, chatname) {
    tibble(
      participant = pid,
      chat_name   = chatname,
      messages    = list(messages_to_tbl(chat$whatsapp_links_with_context))
    )
  })
})


#### Create facebook tibble



# Hilfsfunktion: generische Umwandlung einer Liste von Einträgen in ein Tibble
to_tbl <- function(lst) {
  if (is.null(lst) || length(lst) == 0) return(tibble())
  
  # sammle alle möglichen Felder (weil die mal unterschiedlich heißen können)
  all_fields <- unique(unlist(map(lst, names)))
  
  map_dfr(lst, function(x) {
    vals <- map(all_fields, ~ x[[.x]] %||% NA_character_)
    tibble(!!!set_names(vals, all_fields))
  })
}

# Hauptfunktion: extrahiere pro Teilnehmer seine Facebook-Daten
facebook_tbl <- imap_dfr(master_list, function(pdata, pid) {
  if (is.null(pdata$facebook)) return(NULL)
  
  fb <- pdata$facebook
  
  tibble(
    participant = pid,
    datatype = c("comments", "likes", "follows", "pages"),
    data = list(
      to_tbl(fb$facebook_comments),
      to_tbl(fb$facebook_likes_and_reactions),
      to_tbl(fb$facebook_follows),
      to_tbl(fb$facebook_followed_pages)
    )
  )
})
fb_comments_tbl <- imap_dfr(master_list, function(pdata, pid) {
  if (is.null(pdata$facebook$facebook_comments)) return(NULL)
  to_tbl(pdata$facebook$facebook_comments) |> mutate(participant = pid)
})

fb_likes_tbl <- imap_dfr(master_list, function(pdata, pid) {
  if (is.null(pdata$facebook$facebook_likes_and_reactions)) return(NULL)
  to_tbl(pdata$facebook$facebook_likes_and_reactions) |> mutate(participant = pid)
})

fb_follows_tbl <- imap_dfr(master_list, function(pdata, pid) {
  if (is.null(pdata$facebook$facebook_follows)) return(NULL)
  to_tbl(pdata$facebook$facebook_follows) |> mutate(participant = pid)
})

fb_pages_tbl <- imap_dfr(master_list, function(pdata, pid) {
  if (is.null(pdata$facebook$facebook_followed_pages)) return(NULL)
  to_tbl(pdata$facebook$facebook_followed_pages) |> mutate(participant = pid)
})

# ---------------- Save ----------------
saveRDS(master_list, file = "01_Data/parsed_data.rds")
saveRDS(whatsapp_tbl, file = "01_Data/whatsapp_data_tibble.rds")

saveRDS(fb_comments_tbl, file = "01_Data/facebook_comments_tibble.rds")
saveRDS(fb_likes_tbl,    file = "01_Data/facebook_likes_tibble.rds")
saveRDS(fb_follows_tbl,  file = "01_Data/facebook_follows_tibble.rds")
saveRDS(fb_pages_tbl,    file = "01_Data/facebook_followed_pages_tibble.rds")

# ---------------- Summary ----------------




message("\n✅ Parsing finished!")
message("Files processed: ", file_count)
message("Unique participants: ", length(master_list))
message("Facebook exports loaded: ", facebook_count)
message("WhatsApp exports loaded: ", whatsapp_count)
message("Files with no usable data: ", missing_count)
message("Files with parse errors: ", error_count)





