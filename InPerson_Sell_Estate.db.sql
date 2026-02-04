BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS estate (id INTEGER PRIMARY KEY AUTOINCREMENT, year INT, quarter INT, auction_type varchar(10), number_auction INT, number_asset INT, region varchar(20), city varchar(20), asset_type varchar(10), instrument_type varchar(10), total_sales decimal(12,2));
INSERT INTO "estate" ("id","year","quarter","auction_type","number_auction","number_asset","region","city","asset_type","instrument_type","total_sales") VALUES (1,2025,1,'حضوري',2,9,'الرياض','الرياض','عقارات','أخرى',256450000),
 (2,2025,1,'حضوري',3,14,'الرياض','الرياض','عقارات','سكني',114250000),
 (3,2025,1,'حضوري',1,1,'الرياض','الرياض','عقارات','غير محدد',13600000),
 (4,2025,1,'حضوري',1,1,'الرياض','الدرعيه','عقارات','أخرى',15750000),
 (5,2025,2,'حضوري',1,5,'مكة المكرمة','جده','عقارات','سكني',15500000),
 (6,2025,2,'حضوري',1,2,'الرياض','الرياض','عقارات','سكني',15400000),
 (7,2025,4,'حضوري',1,2,'الرياض','الرياض','عقارات','سكني',8250000),
 (8,2025,4,'حضوري',1,8,'الرياض','الرياض','عقارات','أخرى',22200000),
 (9,2025,4,'حضوري',1,1,'الرياض','الرياض','عقارات','غير محدد',2050000);
COMMIT;
