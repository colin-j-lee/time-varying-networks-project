library(stringr)
library(lubridate)
library(plyr)
library(tidyverse)
library(furrr)
library(googlesheets4)
library(vctrs)
# 
# extract_fun <- function(x){
#   z <- str_split(x, "[,]")[[1]]
#   if(grepl("undefined", z[2])){z <- z[-2]}
#   z <- str_replace(z, '\":\"\"', '":0"')
#   len <- length(z)
#   str_c(combine(lapply(1:len, function(a){str_split(str_replace_all(z[a], '["]', ""),":")[[1]][2]})), collapse=",")
# }
# 
# clean_fun <- function(x){
#   if(any(colnames(x) == "0") | x$type == "RAT"){
#     if(any(colnames(x) == "0")){
#     x <- x %>% select(-item) %>% rename(item = `0`)}
#   }
#   if(x$trial_type == "survey-checkbox"){
#     x <- x %>% mutate(responses2 = str_replace_all(responses2, ",,", ",0,"))
#   }
#   y <- x  %>% select(item, trait, facet, answer, type, responses2)
#   cols <- colnames(y)[apply(y, 2, function(x) grepl("[,]", x))]
#   cols <- which(colnames(y) %in% cols)
#   if(length(cols) > 0){
#     y <- y %>% separate_rows(cols, sep = ",", convert = T)
#   }
#   if(any(is.na(y$answer))){y <- y %>% mutate(answer = NA_character_)}
#   y <- y %>% select(item, trait, facet, answer, type, responses2) %>%
#     mutate_all(funs(ifelse(is.na(.) == T, NA_character_, .))) %>%
#     mutate(SID = sub, Date = date, Hour = hour, Minute = minute,
#            responses2 = as.character(responses2))
# }

wave <- "W1"
wd <- "~/Documents/projects/phase-1-studies/06-data/01-raw/02-esm/"
# wd <- "/Volumes/beck/IPCS/data/C1/"
files <- list.files(wd, pattern = ".csv")
# files <- combine(sapply(subs, function(x) files[grepl(x, files)]))
# files <- files[grepl("040501",  files)]
# files <- sample(files, 10)

extract_fun <- function(x, type){
  z <- str_split(x, "[,]")[[1]]
  if(grepl("undefined", z[2])){z <- z[-2]}
  if(type == "survey-checkbox"){
    z <- str_replace(z, '\":\"\"', '":0"')
  }
  len <- length(z)
  z <- str_c(vec_c(lapply(1:len, function(a){str_split(str_replace_all(z[a], '["]', ""),":")[[1]][2]})), collapse=",")
  z <- str_remove_all(z, "\\}")
  return(z)
}

clean_fun <- function(x, sub){
  if(any(colnames(x) == "0") | x$type == "RAT"){
    if(any(colnames(x) == "0")){
      x <- x %>% select(-item) %>% rename(item = `0`)}
  }
  if(x$trial_type == "survey-checkbox"){
    x <- x %>% mutate(responses2 = str_replace_all(responses2, ",,", ",0,"))
  }
  y <- x  %>% select(Date, item, trait, facet, type, responses2) %>%
    mutate_all(~str_remove_all(., "trash1,")) %>%
    mutate_all(~str_remove_all(., "trash,"))
  cols <- colnames(y)[apply(y, 2, function(x) grepl("[,]", x))]
  cols <- which(colnames(y) %in% cols)
  if(length(cols) > 0){
    y <- y %>% separate_rows(cols, sep = ",", convert = T)
  }
  y <- y %>% select(Date, item, trait, facet, type, responses2) %>%
    mutate_all(~ifelse(is.na(.) == T, NA_character_, .)) %>%
    mutate(SID = sub, responses2 = as.character(responses2))
}

read_fun <- function(file){
  # wd <- "06-data/raw/"
  sub <- str_split(file, "_")[[1]][1]
  i <- which(files == file)
  print(sprintf("%s:%s", i, file))
  file <- paste(wd,file,sep="")
  dat_in <- read_csv(file, col_types = cols(date = col_character(), time = col_character())) %>%
    mutate(responses2 = map2(responses, trial_type, extract_fun),
           responses2 = str_replace(responses2, "\\}", ""),
           Date = sprintf("%s %s", date, time)) %>%
    filter(!is.na(type))
  dat_in_long <- if(nrow(dat_in) == 0) {
    NA
  } else{
    dat_in_long <- dat_in %>%
      group_by(trial_index) %>% 
      nest() %>% 
      ungroup() %>%
      mutate(responses2 = map(data, clean_fun, sub = sub)) %>% 
      filter(!is.na(responses2)) %>%
      select(-data) %>%
      unnest(responses2)
  }
  return(dat_in_long)
} 

plan(multisession(workers = 8L))

dat <- future_map(files
# dat <- map(files
                  # , read_fun
                  , possibly(read_fun, NA_real_)
                  , .progress = T)
closeAllConnections()
dat <- reduce(dat[!is.na(dat)], full_join)

gs4_auth(email = "edbeck@ucdavis.edu")
esm_match <- 
  googlesheets4::read_sheet("https://docs.google.com/spreadsheets/d/1mAj9s39W0zyzUCYdqTXXReIPjvGyb2dS73wzRH6S3-k/edit?usp=sharing"
                                       , sheet = "Sheet1"
                                       , col_types = c(`SID` = "c")) %>%
  filter(!is.na(name))
  # # unnest(`Participant ID`) %>%
  # mutate(SID = as.character(`Participant ID`))
2

old <- dat

dat <- dat #%>% filter(!SID %in% c("megan", "emorie"))

all_dat %>% 
  filter(is.na(Date2)) %>% 
  mutate(date = str_remove_all(as.character(Date), " \\d+:\\d+:\\d+"),
         date = parse_date_time(date, orders = c("mdy", "ymd", "dmy"))) %>%
  filter(!is.na(date))

all_dat <- dat %>% 
  arrange(Date) %>%
  # rowwise() %>%
  mutate(Date = parse_date_time(Date, orders = c("mdy_hMS %p", "ymd_hMS %p", "dmy_hMS %p", '%Y/%m/%d %I:%M:%S', '%Y/%m/%d %H:%M:%S'))) %>%
  group_by(SID) %>% 
  arrange(Date) %>%
  mutate(StartDate = min(Date), 
         Day = as.numeric(difftime(Date,StartDate), units = "days"),
         Hour = hour(Date)) %>%
  ungroup() %>%
  left_join(esm_match %>% select(SID, starts_with("HourBlock"))) %>%
  ungroup() %>%
  mutate_at(vars(starts_with("HourBLock")), as.numeric) %>%
  mutate(HourBlock1max = HourBlock2 - 1
         , HourBlock1max = ifelse(HourBlock1max < 0, HourBlock1max + 24, HourBlock1max)
         , HourBlock1mid = HourBlock2 - 2
         , HourBlock1mid = ifelse(HourBlock1mid < 0, HourBlock1mid + 24, ifelse(HourBlock1mid > 24, HourBlock1mid-24, HourBlock1mid))
         , HourBlock2max = HourBlock3 - 1
         , HourBlock2max = ifelse(HourBlock2max < 0, HourBlock2max + 24, HourBlock2max)
         , HourBlock2mid = HourBlock3 - 2
         , HourBlock2mid = ifelse(HourBlock2mid < 0, HourBlock2mid + 24, ifelse(HourBlock2mid > 24, HourBlock2mid-24, HourBlock2mid))
         , HourBlock3max = HourBlock4 - 1
         , HourBlock3max = ifelse(HourBlock3max < 0, HourBlock3max + 24, HourBlock3max)
         , HourBlock3mid = HourBlock4 - 2
         , HourBlock3mid = ifelse(HourBlock3mid < 0, HourBlock3mid + 24, ifelse(HourBlock3mid > 24, HourBlock3mid-24, HourBlock3mid))
         , HourBlock4max = HourBlock5 - 1
         , HourBlock4max = ifelse(HourBlock4max < 0, HourBlock4max + 24, HourBlock4max)
         , HourBlock4mid = HourBlock5 - 2
         , HourBlock4mid = ifelse(HourBlock4mid < 0, HourBlock4mid + 24, ifelse(HourBlock4mid > 24, HourBlock4mid-24, HourBlock4mid))
         , HourBlock5max = HourBlock5 + 1
         , HourBlock5max = ifelse(HourBlock5max < 0, HourBlock5max + 24, HourBlock5max)
         , HourBlock5mid = HourBlock5 + 2
         , HourBlock5mid = ifelse(HourBlock5mid < 0, HourBlock5mid + 24, ifelse(HourBlock5mid > 24, HourBlock5mid-24, HourBlock5mid))) %>%
  mutate(HourBlock = 
           ifelse(Hour %in% c(HourBlock1, HourBlock1mid, HourBlock1max), 1,
           ifelse(Hour %in% c(HourBlock2, HourBlock2mid, HourBlock2max), 2,
           ifelse(Hour %in% c(HourBlock3, HourBlock3mid, HourBlock3max), 3,
           ifelse(Hour %in% c(HourBlock4, HourBlock4mid, HourBlock4max), 4,
           ifelse(Hour %in% c(HourBlock5, HourBlock5mid, HourBlock5max), 5, NA
  )))))) %>%
  dplyr::rename(value = responses2) %>%
  select(SID, Date, type, trait, facet, item, value, StartDate, Day, Hour, HourBlock)

valid_dates <- all_dat %>%
  select(SID, Date) %>%
  distinct() %>%
  group_by(SID) %>%
  arrange(SID, Date) %>%
  mutate(difftime = as.numeric(difftime(Date,lag(Date), units = "min"))) %>%
  filter(difftime > 5) %>%
  ungroup()

all_dat <- all_dat %>%
  right_join(valid_dates %>% select(-difftime))


set.seed(5)
subs <- tibble(old = unique(all_dat$SID)) %>%
  mutate(new = round(runif(n(), 10000, 99999)))
write_csv(subs, file = "~/Documents/projects/phase-1-studies/06-data/02-parsed/subs.csv")

all_dat <- all_dat %>%
  mutate(SID = mapvalues(SID, subs$old, subs$new))

# dat <- all_dat #%>% filter(min(Date) <= "2018-11-26") 

BFI <- all_dat %>% filter(type == "BFI2") %>% mutate(value = as.numeric(mapvalues(value, 0:4, 1:5)))
uni <- all_dat %>% filter(type == "unique") %>% mutate(value = as.numeric(mapvalues(value, 0:4, 1:5)))
emotion <- all_dat %>% filter(type == "emotion") %>% mutate(value = as.numeric(mapvalues(value, 0:4, 1:5)))
DS8 <- all_dat %>% filter(type == "DIAMONDS") %>% mutate(value = as.numeric(mapvalues(value, 0:2, 1:3)))
sit <- all_dat %>% filter(type == "situation") %>% mutate(value = as.numeric(value))
erstrat <- all_dat %>% filter(type == "erstrat") %>% mutate(value = as.numeric(value))
ergoals <- all_dat %>% filter(type == "ergoals") %>% mutate(value = as.numeric(mapvalues(value, 0:4, seq(-2,2,1))))
narcissism <- all_dat %>% filter(type == "narcissism") %>% mutate(value = as.numeric(mapvalues(value, 0:4, 1:5)))
free_resp <- all_dat %>% filter(type == "freeresp")

dat_wide <- all_dat %>% 
  select(SID, Date, StartDate, Day, Hour, HourBlock, item, value) %>%
  distinct() %>%
  filter(!(value == ")" | value == ")))")) %>%
  pivot_wider(names_from = "item"
              , values_from = "value"
              , names_sort = T) %>%
  mutate_at(vars(-(Date:HourBlock), -freeresp), as.numeric) %>%
  mutate_at(vars(matches("E[0-9]"), matches("A[0-9]"), matches("C[0-9]"), matches("N[0-9]"), matches("O[0-9]")
                 , matches("EM[0-9]"), matches("NR[0-9]"), matches("D[0-9]"), matches("U[0-9]")), ~(.) + 1) %>%
    # select(matches("E[0-9]"))
  mutate_at(vars(starts_with("ergoal")), ~(.) - 2)


save(all_dat, dat, dat_wide, file = sprintf("~/Documents/projects/phase-1-studies/06-data/02-parsed/all_clean_data_%s_%s.RData", wave, Sys.Date()))
save(BFI, uni, emotion, DS8, sit, erstrat, ergoals, narcissism, free_resp
     , file = sprintf("~/Documents/projects/phase-1-studies/06-data/02-parsed/type_clean_data_%s_%s.RData", wave, Sys.Date()))
write_csv(dat_wide, file = sprintf("~/Documents/projects/phase-1-studies/06-data/02-parsed/wide_clean_data_%s_%s.csv", wave, Sys.Date()))
