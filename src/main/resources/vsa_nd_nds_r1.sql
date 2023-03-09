CREATE TABLE IF NOT EXISTS `tmp_tos`.`vsa_nd_nds_r1`(
    `fid` decimal(38,0),
    `year` decimal(38,0),
    `quarter` decimal(38,0),
    `date_receipt` date,
    `s40` decimal(38,0),
    `code_period` string,
    `code_present_place` string
)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
