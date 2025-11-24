from com.lemontree.runners.base.base_runner import BaseJobRunner


def calculate_mat(self, fact_hotel_tags_df, md_hotels_df, dim_source_segment_df, current_date) -> BaseJobRunner.DataFrame:

    F = self.F
    # Convert business_date to date type
    fact_hotel_tags_df = fact_hotel_tags_df.\
        withColumn("business_date", F.to_date("business_date", "dd-MM-yyyy")). \
        filter(F.col("business_date") == current_date)

    # Step 2: Join with dimension tables
    transformed_df = (
        fact_hotel_tags_df
        .join(md_hotels_df, "hotel_code", "left")
        .join(dim_source_segment_df, "src_sgmnt_id", "left")
        .select(
            F.col("business_date").alias("as_of_date"),
            F.col("hotel_id"),
            F.col("hotel_code"),
            F.col("no_of_rooms").alias("inventory"),
            F.col("source").alias("source_nm"),
            F.col("segment").alias("segment_nm"),
            F.col("room_nights"),
            F.col("room_revenue")
        )
    )

    # Step 3: Aggregate MAT metrics
    aggregated_df = transformed_df.groupBy(
         "as_of_date", "hotel_id", "hotel_code", "inventory","source_nm","segment_nm"
    ).agg(
        F.sum("room_nights").alias("Total_rooms"),
        F.sum("room_revenue").alias("total_room_revenue")
    )

    return aggregated_df