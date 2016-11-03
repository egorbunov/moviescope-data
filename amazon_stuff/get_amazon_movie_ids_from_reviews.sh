#! /bin/bash

# just scans all text files in reviews dirs
# and greps amazon product id (film id) and asin id from them

REVIEW_PROD_FILES_DIR="$HOME/raw/amazon/m_prod_id_reviews"
REVIEW_ASIN_FILES_DIR="$HOME/raw/amazon/m_asin_id_reviews"
PATH_TO_AMAZON_IDS="$HOME/raw/amazon/movie_ids"
RESULT_IDS_FILE="IDS.txt"

function collect_amazon_prod_ids {
    input_reviews_file="$1"
    grep "product/productId:" "$input_reviews_file" | \
        awk '{print $2}' | \
        sort | uniq
}

function collect_amazon_asin_ids {
    grep -Po "\"asin\": \"\K(\d+)(?=\")" "$1" |  sort | uniq 
}

mkdir -p $PATH_TO_AMAZON_IDS && \
rm -rf $PATH_TO_AMAZON_IDS/* && \
echo "Output directory (cleaned): $PATH_TO_AMAZON_IDS" && \
export -f collect_amazon_prod_ids && \
export -f collect_amazon_asin_ids && \
echo "Prasing prod ids from reviews in $REVIEW_PROD_FILES_DIR ..." && \
find "$REVIEW_PROD_FILES_DIR" -type f | \
    parallel --no-notice collect_amazon_prod_ids {} '>' $PATH_TO_AMAZON_IDS/ids-{/.}.txt && \
echo "Parsing asin ids from reviews in $REVIEW_ASIN_FILES_DIR ..." && \
find "$REVIEW_ASIN_FILES_DIR" -type f | \
    parallel --no-notice collect_amazon_asin_ids {} '>' $PATH_TO_AMAZON_IDS/ids-{/.}.txt && \
echo "Merging all ids to one file..." && \
cat $PATH_TO_AMAZON_IDS/* | sort | uniq > $PATH_TO_AMAZON_IDS/$RESULT_IDS_FILE && \
find "$PATH_TO_AMAZON_IDS" ! -name "$RESULT_IDS_FILE" -type f -exec rm -f {} \; && \
echo "Done, final ids file: $PATH_TO_AMAZON_IDS/$RESULT_IDS_FILE" 
