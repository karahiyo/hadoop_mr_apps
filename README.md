

# Run app

--------------

ˆê’i–Ú
```
# hadoop jar CalcRegionAggregation.jar sample.mr.JoinSalesDataJob /data/sales_sample/sales /data/sales_sample/sales_detail /output/job1
```

“ñ’i–Ú
```
# hadoop jar CalcRegionAggregation.jar sample.mr.JoinRegionAndItemJob /output/job1 /output/job2 /data/sales_sample/shoplist/shoplist.tsv /data/sales_sample/itemlist/itemlist.tsv
```

