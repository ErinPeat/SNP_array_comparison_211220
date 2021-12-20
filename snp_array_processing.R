library(tidyverse)


setwd("/home/erin/Documents/Work/SNP_array_liftover")

ordered_chrs <- c("chr1",  "chr2",  "chr3"  ,"chr4"  ,"chr5" , "chr6" , "chr7",  "chr8",
"chr9" , "chr10", "chr11", "chr12", "chr13", "chr14", "chr15", "chr16",
"chr17" ,"chr18" ,"chr20", "chr21" ,"chr22", "chrX" , "chrY")


means_df <- read.delim("df_means_211218.csv", sep = '\t' , header = TRUE)  

means_df_longer <- means_df %>% 
  pivot_longer(cols = all_of(ordered_chrs), names_to = "chromosome", values_to = "num_probes") %>% 
  #filter(num_probes != 0) %>% 
  group_by(chromosome) %>% 
  mutate(lower_range = min(num_probes, na.rm = TRUE),
         upper_range = max(num_probes, na.rm = TRUE),
         mean = signif(mean(num_probes, na.rm = TRUE),3),
         median = median(num_probes, na.rm = TRUE),
         chromosome = factor(chromosome, levels = ordered_chrs, ordered = TRUE),
         less_than_three = sum(num_probes <= 3,  na.rm = TRUE),
         row_count = sum(!is.na(num_probes)),
         perc_less_than_three = signif((less_than_three/row_count)*100,3)) %>% 
  select(-c(num_probes)) %>% 
  distinct() %>% 
  arrange(chromosome)


write.table(means_df_longer, "means_range_211218.txt", sep ='\t', col.names = TRUE, row.names = FALSE, quote = FALSE)

