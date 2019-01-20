#############################
# Functions US data.gov API #
#############################

# Libraries
require(tidyverse) # Data cleaning
require(tibble) # for is_tibble()
require(rvest) # Basic scraping
require(magrittr) # for %<>% pipe
require(lubridate) # manipulating times/dates
require(purrr) # Mapping functions
require(jsonlite) # API interfacing
require(memoise) # Caching
require(furrr) # Parallel processing

#===========================================
# Cache fromJSON to make calls safer/faster
#===========================================

.fromJSON <- memoise(fromJSON)

#===============================================
# Functions for API key:
# 1. requesting a new api key
# 2. registering api_key for scraping session
#===============================================
# request_apikey <- function(first_name = NULL, last_name = NULL, email = NULL) {
#   # params check
#   if(is.null(first_name)|is.null(last_name)|is.null(email)) stop('Must supply first/last name and email!')
#   
#   # USA data.gov API key request url: https://api.data.gov/signup/
#   api_reg_url <- 'https://api.data.gov/signup/'
#   api_reg_sess <- html_session(api_reg_url)
#   
#   # API registration form
#   unfilled <- api_reg_sess %>% 
#     html_node('form') %>% 
#     html_form()
#   filled <- unfilled %>% 
#     set_values('user[first_name]' = first_name,
#                'user[last_name]' = last_name,
#                'user[email]' = email)
#   api_reg_sess %>% 
#     submit_form(., filled)
# }
register_apikey <- function(key = NULL) {
  # Check params
  if(is.null(key)) stop('Must supply an API key.')
  cat('List of ways to pass API key: https://api.data.gov/docs/api-key/\n',
      'OR directly register here: https://api.govinfo.gov/docs/')
  
  # Combine call parts
  api <- 'api_key='
  key <- key
  api_key <- paste0(api,key)
  
  # Assign to environment
  api_key <<- api_key
}

#=============================================
# Function to show available bulk data
#=============================================
show_collections <- function(key = api_key) {
  # Collections URL
  collections_url <- 'https://api.govinfo.gov/collections'
  
  # Get collections
  call <- .fromJSON(paste(collections_url, api_key, sep="?"))
  df <- call$collections
  return(df)
}

#=========================================================================
# Function that gathers meta data for any collection of documents
#=========================================================================
# API call to collection requires a couple parameters:
# 1. collectionCode
# 2. startDate
# 3. offset
# 4. pageSize
# 5. endDate (optional, may be needed to restrict call from exceeding 10000 item limit)

load_collection <- function(collection, start_date = NULL, end_date = NULL, key = api_key, verbose = T) {
  # Collections URL
  collections_url <- 'https://api.govinfo.gov/collections'
  
  # Set request parameters:
  # Collection code
  coll_code <- collection
  if(is.null(coll_code)) stop('Must supply collection code!')
  
  # Start/end dates
  if(is.null(start_date)){
    start <- '2017-01-01T00:00:00Z' %>% as_datetime()
  } else {
    start <- start_date %>% as_datetime()
  }
  if(is.null(end_date)) {
    end <- Sys.time() %>% as_datetime()
  } else {
    end <- end_date %>% as_datetime()
  }
  if(end-start < 0) stop('Start date must temporally precede end date!')
  
  # Offset and page size
  offset <- 0 
  n_items <- 10000 # Items per page (max = 10000)
  
  ##############################################################
  ##############################################################
  ## If collection package count > 10,000 automatically parse ##
  ## time period into acceptable sub periods                  ##
  ##############################################################
  ##############################################################
  collections <- show_collections()
  pkgCount <- collections[['packageCount']][which(collections$collectionCode==coll_code)]
  
  if(pkgCount > 10000) {
    if(verbose) cat('Seeding date periods...\n')
    
    # Convert into non-overlapping sequences of date times
    start_seq <- seq(start, end, by = 'months')
    end_seq <- (start_seq - duration(seconds = 1)) %>% .[2:length(.)] %>% c(.,end)
    
    # Convert to character sequences in data frame that can be looped over in API call
    dates <- data.frame(
      start_seq = start_seq %>% as.character() %>% paste0(.,"T00:00:00Z"),
      end_seq = end_seq %>% as.character() %>% gsub(' ', 'T', .) %>% paste0(., 'Z'),
      stringsAsFactors = F
    )
    
    #########################################
    # Loop API call over generic seed dates #
    #########################################
    for (row in 1:nrow(dates)) {
      # create API request head
      request_head <- paste(collections_url, coll_code, dates[row,1], dates[row,2], sep ='/')
      
      # Create API request tail
      os <- paste0('offset=', offset)
      ps <- paste0('pageSize=',n_items)
      request_tail <- paste(os, ps, key, sep = '&')
      
      # combine full API request
      request <- paste(request_head, request_tail, sep='/?')
      
      # Call API
      for (attempt in 1:10){
        try({
          # Make request
          collection_request <- .fromJSON(request)
          break #break/exit for-loop
        }, silent = T)
      }
      
      # Print bill count in period
      dates$n_pkg[row] <- collection_request$count
      if(verbose) cat(coll_code , 'count in period', dates[row,1], '--', dates[row,2], 'is', collection_request$count, '\n', sep = ' ')
    }
    
    ###############################################
    # Automatically find feasible request periods #
    ###############################################
    step <- 1
    while(any(dates$n_pkg > 10000)) {
      # Print step
      if(verbose) cat('Iteration', step, '\n', sep = ' ')
      
      # Drops rows with 0 packages
      dates %<>% filter(n_pkg!=0)
      
      # Split periods with n_pkg> 10000
      exceed <- which(dates$n_pkg > 10000)
      for(i in 1:length(exceed)) {
        # Rename index
        row <- exceed[i]
        
        # Padding to account for index displacement as new rows added
        j <- length(which(is.na(dates$n_pkg)==T)) 
        dis <- ifelse(j > 0, j - i + 1, 0L)
        
        # Calculate new time periods
        scale_factor <- 1/(10000/(1.1^i))
        secs <- ceiling(int_length(dates$start_seq[row+dis] %--% dates$end_seq[row+dis]) / (dates$n_pkg[row+dis] * scale_factor))
        time_unit <- paste(secs, 'secs', sep = ' ')
        
        # Convert into non-overlapping sequences of date times
        start_seq <- seq(as_datetime(dates$start_seq[row+dis]), as_datetime(dates$end_seq[row+dis]), by = time_unit)
        end_seq <- (start_seq - duration(seconds = 1)) %>% .[2:length(.)] %>% c(., as_datetime(dates$start_seq[row+dis+1]) - duration(seconds = 1))
        
        # Convert to character sequences in data frame that can be looped over in API call
        sub_dates <- data.frame(
          start_seq = start_seq %>% as.character() %>% gsub(' ', 'T', .) %>% paste0(., 'Z'),
          end_seq = end_seq %>% as.character() %>% gsub(' ', 'T', .) %>% paste0(., 'Z'),
          n_pkg = NA,
          stringsAsFactors = F
        )
        
        # Insert new periods into previous dates df
        if(row == 1) {
          dates_tail <- dates[(row+1):nrow(dates),]
          dates <- rbind(sub_dates, dates_tail)
        } else {
          dates_head <- dates[1:(row+dis-1),]
          dates_tail <- dates[(row+dis+1):nrow(dates),]
          dates <- rbind(dates_head, sub_dates, dates_tail)
        }
      }
      
      # Update missing pkg counts
      new_rows <- which(is.na(dates$n_pkg))
      for (i in 1:length(new_rows)) {
        
        # Rename index
        nr <- new_rows[i] 
        
        # Create API request head
        request_head <- paste(collections_url, coll_code, dates[nr,1], dates[nr,2], sep ='/')
        
        # Create API request tail
        os <- paste0('offset=', offset)
        ps <- paste0('pageSize=', n_items)
        request_tail <- paste(os, ps, key, sep = '&')
        
        # Combine full request
        request <- paste(request_head, request_tail, sep='/?')
        
        # Call API
        for (attempt in 1:10){
          try({
            # Make request
            collection_request <- .fromJSON(request)
            break #break/exit for-loop
          }, silent = T)
        }
        
        # Update counts
        dates$n_pkg[nr] <- collection_request$count
        
        # Print updated bill counts
        if(verbose) cat(coll_code, 'count in period', dates[i,1], '--', dates[i,2], 'is', collection_request$count, '\n', sep = ' ')
      }
      # Update step number
      step <- step + 1
    }
    
    #########################
    # Retrieve all packages #
    #########################
    pkg_summary <- list(NULL)
    for (row in 1:nrow(dates)){
      
      # Create API request head
      request_head <- paste(collections_url, coll_code, dates[row,1], dates[row,2], sep ='/')
      
      # Create API request tail
      os <- paste0('offset=', offset)
      ps <- paste0('pageSize=', n_items)
      request_tail <- paste(os, ps, key, sep = '&')
      
      # Combine full request
      request <- paste(request_head, request_tail, sep='/?')
      
      # Call API
      for (attempt in 1:10){
        try({
          # Make request
          collection_packages <- .fromJSON(request)
          break #break/exit for-loop
        }, silent = T)
      }
      
      # Extract package data
      pkg_summary[[row]] <- collection_packages$packages %>%
        mutate(lastModified = as_datetime(lastModified))
    }
    
    # Collapse list into data frame
    pkg_meta <- bind_rows(pkg_summary)
    
  } else {
    ########################
    ########################
    ## Else download pkgs ##
    ########################
    ########################
    # Reformat dates
    start %<>% as.character(); end %>% as.character()
    start <- ifelse(grepl('\\:', start), gsub(' ', 'T', start) %>% paste0(.,'Z'), paste0(start, 'T00:00:00Z'))
    end <- ifelse(grepl('\\:', end), gsub(' ', 'T', end) %>% paste0(.,'Z'), paste0(end, 'T00:00:00Z'))
    
    # Create API request head
    request_head <- paste(collections_url, coll_code, start, end, sep ='/')
    
    # Create API request tail
    os <- paste0('offset=', offset)
    ps <- paste0('pageSize=', n_items)
    request_tail <- paste(os, ps, key, sep = '&')
    
    # Combine full request
    request <- paste(request_head, request_tail, sep='/?')
    
    # Call API
    for (attempt in 1:10){
      try({
        # Make request
        collection_packages <- .fromJSON(request)
        break #break/exit for-loop
      }, silent = T)
    }
    
    # Extract package metadata
    pkg_meta <- collection_packages$packages %>%
      mutate(lastModified = as_datetime(lastModified))
  }
  
  #####################################
  # Assign pkg_summary to environment #
  #####################################
  class_type <- paste(tolower(coll_code), 'collection', sep = '_')
  class(pkg_meta) <- append(class(pkg_meta), class_type)
  assign(paste(tolower(coll_code), 'metadata', 'df', sep = '_'),
         pkg_meta,
         envir = .GlobalEnv)
}

#==========================================
# Function to retrieve bill packages
#==========================================
bill_reader <- function(url, key = api_key) {
  
  # api key and URL message
  if(is.null(api_key)|is.null(url)) stop('Must supply a URL and API key.')
  
  # Request individual bill data
  request <- url %>% paste(., key, sep = '?')
  for (attempt in 1:50){
    try({
      # Make request
      bill <- .fromJSON(request)
      break #break/exit for-loop
    }, silent = T)
  }
  
  # Sponsor & cosponsors
  if(is.null(bill$members)) {
    sponsor <- cosponsors <- NA
  } else {
    sponsor <- bill$members %>% 
      filter(role=='SPONSOR') %>% 
      paste0(' [', .$chamber, '; ', .$party, '-', .$state, ']') %>% 
      .[1] %>% 
      as.character()
    cosponsors <- bill$members %>% 
      filter(role!='SPONSOR') %>% list()
  }
  
  # Committees
  committees <- bill$committees$committeeName %>% list()
  
  # Related Bills
  if(is.null(bill$related)) {
    related_bills <- NA
  } else {
    related_bills <- bill$related %>% list()
  }
  
  # References
  if(is.null(bill$references)) {
    references <- NA
  } else {
    references <- bill$references %>% list()
  }
  
  # Links
  links <- bill$download %>% list()
  
  # Store desired items in data frame with list-columns
  df <- tibble(
    congress = bill$congress %>% as.numeric(),
    bill_type = bill$billType,
    bill_num = bill$billNumber,
    date = bill$dateIssued %>% as_datetime(),
    session = bill$session,
    sponsor = sponsor,
    origin_chamber = bill$originChamber,
    current_chamber = bill$currentChamber,
    committees = committees,
    cosponsors = cosponsors,
    related_bills = related_bills,
    references = references,
    version = bill$billVersion,
    package_ID = bill$packageId,
    links = links,
    appropriation = ifelse(bill$isAppropriation=='true', T, F),
    private = ifelse(bill$isPrivate=='true', T, F)
  )
  return(df)
}

#=============================================================
# Function to extract package data from metadata df
# - This is a generic function that identifies the metadata
#   collection type (e.g. BILLS, USCOURTS, etc.) and calls
#   the requisite extraction function
# - It returns an object type commensurate with the data type
# - So far, only written bill_reader, but additional functions
#   easily included
#=============================================================
extract_content <- function(df, ...) {
  # Check collection type
  c_type <- class(df)[grep('collection', class(df))]
  if(is_empty(c_type)) stop('Data frame must be a package metadata object of class "[COLLECTION]_container" created by load_collection() function.')
  
  # Filter 
  df %<>% filter_(...)
  
  # Links
  url <- df['packageLink'] %>% .[,1]
  
  # Set up parallel environment
  plan(multiprocess)
  
  # Select appropriate function 
  # can extend to other collection types with simple else(if) extension
  if(grepl('bills', c_type)) {
    name <- c_type %>% str_split(.,'_') %>% unlist() %>% .[1]
    f <- safely(get('bill_reader', envir = .GlobalEnv), otherwise = NA_real_)
  }
  
  # Extract data
  list <- future_map(url, ~ f(url = .x, key = get('api_key', envir = .GlobalEnv)))
  
  # Extract results
  results <- list %>%
    map(., ~ .x$result)
  
  # Index missing results
  k <- results %>%
    map(., ~ !is_tibble(.x)) %>%
    unlist() %>%
    which(.==T)
  
  # Drop from list
  results[k] <- NULL
  
  # Identify missing links from original metadata_df
  missing <- df[k,]
  
  # Rewrite df as results
  df <- results %>% bind_rows()
  
  # Assign missing_df and results_df to environment
  assign(paste(name, 'alldata', sep = '_'),
         df,
         envir = .GlobalEnv)
  assign('broken_links_df',
         missing,
         envir = .GlobalEnv)
}