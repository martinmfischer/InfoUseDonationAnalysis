# ------------------------------------------------------------
# Script: parse_all_json_robust.R
# Task  : Robustly load all JSONs in 01_Data, extract params,
#         flag malformed/empty files, save combined df
# ------------------------------------------------------------

library(jsonlite)
library(dplyr)
library(stringr)
library(tibble)

# files
files <- list.files("01_Data", pattern = "\\.json$", full.names = TRUE)

# extract params from basename
extract_params <- function(fname) {
  bn <- basename(fname)
  participant <- str_match(bn, "participant=([^_]+)")[,2]
  key         <- str_match(bn, "key=([^\\.]+)")[,2]
  list(participant = participant, key = key)
}

# try to locate 'whatsapp_links_with_context' in parsed JSON
find_links <- function(j) {
  if (is.null(j)) return(NULL)
  
  # direct field
  if (is.list(j) && !is.null(j$whatsapp_links_with_context)) {
    return(j$whatsapp_links_with_context)
  }
  
  # data.frame with column
  if (is.data.frame(j) && "whatsapp_links_with_context" %in% names(j)) {
    return(j$whatsapp_links_with_context[[1]])
  }
  
  # list of elements that may contain the field
  if (is.list(j)) {
    matches <- lapply(j, function(x) {
      if (is.list(x) && !is.null(x$whatsapp_links_with_context)) {
        return(x$whatsapp_links_with_context)
      }
      NULL
    })
    matches <- matches[!sapply(matches, is.null)]
    if (length(matches) > 0) return(matches[[1]])
  }
  
  # fallback: check first element safely
  if (is.list(j) && length(j) >= 1) {
    el1 <- j[[1]]
    if (is.list(el1) && !is.null(el1$whatsapp_links_with_context)) {
      return(el1$whatsapp_links_with_context)
    }
  }
  
  return(NULL)
}


results <- list()

for (f in files) {
  params <- extract_params(f)
  # safe parse
  parsed <- tryCatch(fromJSON(f, simplifyVector = FALSE), error = function(e) e)
  
  if (inherits(parsed, "error")) {
    results[[length(results) + 1]] <- tibble(
      link = NA_character_,
      domain = NA_character_,
      participant = params$participant,
      key = params$key,
      file = basename(f),
      status = "invalid_json",
      error_msg = parsed$message
    )
    next
  }
  
  links <- find_links(parsed)
  
  if (is.null(links) || length(links) == 0) {
    results[[length(results) + 1]] <- tibble(
      link = NA_character_,
      domain = NA_character_,
      participant = params$participant,
      key = params$key,
      file = basename(f),
      status = "no_links_found",
      error_msg = NA_character_
    )
    next
  }
  
  # coerce links -> dataframe
  df_links <- NULL
  if (is.data.frame(links)) {
    df_links <- as_tibble(links)
  } else if (is.list(links)) {
    df_links <- tryCatch(bind_rows(links), error = function(e) NULL)
    if (is.null(df_links)) {
      df_links <- tryCatch(as_tibble(links), error = function(e) NULL)
    }
  } else {
    df_links <- tibble(link = as.character(links))
  }
  
  if (is.null(df_links) || nrow(df_links) == 0) {
    results[[length(results) + 1]] <- tibble(
      link = NA_character_,
      domain = NA_character_,
      participant = params$participant,
      key = params$key,
      file = basename(f),
      status = "bad_structure",
      error_msg = "could not convert links to dataframe"
    )
    next
  }
  
  # normalize columns
  if (!"link" %in% names(df_links)) df_links$link <- NA_character_
  if (!"domain" %in% names(df_links)) df_links$domain <- NA_character_
  
  df_links <- df_links %>%
    mutate(
      link = as.character(link),
      domain = as.character(domain),
      participant = params$participant,
      key = params$key,
      file = basename(f),
      status = "ok",
      error_msg = NA_character_
    )
  
  results[[length(results) + 1]] <- df_links
}

# combine and save
df_all <- bind_rows(results)

save(df_all, file = "01_Data/all_whatsapp_links.RData")
# optional quick inspect
write.csv(df_all, "01_Data/all_whatsapp_links.csv", row.names = FALSE)

# brief summary
cat("files:", length(files), " | ok:", sum(df_all$status == "ok", na.rm = TRUE),
    " | invalid/missing:", sum(df_all$status != "ok", na.rm = TRUE), "\n")
