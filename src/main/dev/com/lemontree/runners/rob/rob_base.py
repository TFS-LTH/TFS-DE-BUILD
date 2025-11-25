from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.constants.constants import ROOM_TYPES, PRICE_GROUP_TYPES, RESERVATION_STATUS, \
    RESERVATION_STATUS_MAPPING


def calculate_rob(self, fact_reservation_df, md_hotels_df, protel_reservation_df, source_segment_df,
                  current_date) -> BaseJobRunner.DataFrame:
    F = self.F
    W = self.W
    # ----------------------------
    # Step 1: Filter reservations loaded up to today
    # ----------------------------
    filtered_rsrv_curr_month = (
        protel_reservation_df.join(md_hotels_df, "hotel_id", "left")
        .filter(F.col("load_datetime") <= F.lit(current_date))
        .select(
            "bkg_num",
            "load_datetime",
            "arrival_dt",
            "departure_dt",
            "reservation_status",
            "room_typ",
            "rm_rate",
            "rm_rate_wo_split",
            "hotel_id",
            "price_typ_grp",
            "num_of_room_booked",
            "hotel_code",
            "no_of_rooms",
            "owned_vs_managed"
        )
    )

    # ----------------------------
    # Step 2: Rank reservations to get the latest per booking
    # ----------------------------
    window_spec = W.partitionBy("bkg_num").orderBy(F.col("load_datetime").desc())
    ranked_rsrv_curr_month = filtered_rsrv_curr_month.withColumn("rnk", F.rank().over(window_spec))

    # ----------------------------
    # Step 3: Keep only latest reservations and adjust dates
    # ----------------------------
    dated_rsrv_curr_month = ranked_rsrv_curr_month.filter(
        (F.col("rnk") == 1) & (F.col("departure_dt") >= F.lit(current_date)) \
        & (F.col("departure_dt") > F.col("arrival_dt"))). \
        withColumn("arrival_dt_new", F.col("arrival_dt")).withColumn("departure_dt_new", F.col("departure_dt"))

    # ----------------------------
    # Step 4: Explode reservations into one row per stay date
    # ----------------------------

    rns_for_rsrv_curr_month = dated_rsrv_curr_month.alias("drcm"). \
        withColumn("stay_date", F.explode(F.sequence(F.col("arrival_dt_new").cast("date"), \
                                                     F.date_sub(F.col("departure_dt_new").cast("date"), 1),
                                                     F.expr("interval 1 day")))) \
        .join(fact_reservation_df, dated_rsrv_curr_month["bkg_num"] == fact_reservation_df["source_rsrv_id"], "left") \
        .join(source_segment_df, fact_reservation_df["src_sgmnt_id"] == source_segment_df["src_sgmnt_id"], "left") \
        .withColumn("room_rate", F.coalesce(
        F.col("drcm.rm_rate").cast("decimal(10,2)"),
        F.col("drcm.rm_rate_wo_split").cast("decimal(10,2)")
    )) \
        .filter(
        (F.col("reservation_status").isin(RESERVATION_STATUS)) &
        (~F.col("drcm.room_typ").isin(ROOM_TYPES)) &
        (~F.col("price_typ_grp").isin(PRICE_GROUP_TYPES))
    ) \
        .select(
        "drcm.hotel_id",
        "drcm.hotel_code",
        "drcm.no_of_rooms",
        "reservation_status",
        "owned_vs_managed",
        "bkg_num",
        "arrival_dt_new",
        "departure_dt_new",
        "source_rsrv_id",
        fact_reservation_df["src_sgmnt_id"].alias("src_sgmnt_id"),
        "room_rate",
        "stay_date",
        "num_of_room_booked",
        source_segment_df["source"],
        source_segment_df["segment"]
    )

    # ----------------------------
    # Step 5: Aggregate to get rob
    # ----------------------------
    rob = (
        rns_for_rsrv_curr_month
        .groupBy("stay_date", "hotel_id", "hotel_code", "no_of_rooms", "source", "segment", "reservation_status",
                 "owned_vs_managed")
        .agg(
            F.sum("num_of_room_booked").alias("rob"),
            F.sum(F.col("num_of_room_booked") * F.col("room_rate")).alias("room_revenue")
        )
        .withColumn("as_of_date", F.lit(current_date))
        .withColumnRenamed("no_of_rooms", "inventory")
        .withColumnRenamed("source", "source_nm")
        .withColumnRenamed("segment", "segment_nm")
    )

    rob = rob.filter(F.col("stay_date") >= F.lit(current_date))

    rob = rob.replace(RESERVATION_STATUS_MAPPING, subset=["reservation_status"]) \
        .select(
        self.F.col("as_of_date").cast("date").alias("as_of_date"),
        self.F.col("stay_date").cast("date").alias("stay_date"),
        "hotel_id",
        "hotel_code",
        "inventory",
        "source_nm",
        "segment_nm",
        "reservation_status",
        "room_revenue",
        "rob"
    )

    return rob
