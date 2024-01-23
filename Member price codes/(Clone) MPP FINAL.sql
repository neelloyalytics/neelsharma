-- Databricks notebook source
-- comment 

-- COMMAND ----------

select * from gold.material_master where material_id in ('690859',
'647918'
) ; 

-- comment

-- COMMAND ----------

select distinct comp from sandbox.sr_mpp_promo_items_list_a  ; 

-- COMMAND ----------

select * from sandbox.sr_mpp_no_discount_items_list ;

-- COMMAND ----------



-- COMMAND ----------

drop table if exists sandbox.ns_mpp_all;
create table sandbox.ns_mpp_all as

select department_name , category_name , material_group_name, brand , material_id , material_name , final_segment , 'old_promo' as promo_type
from  sandbox.sr_mpp_promo_items_list_a where comp <> 'doubtful' 

union 

select department_name , category_name , material_group_name, brand , material_id , material_name , final_segment , 'new_items' as promo_type

from sandbox.sr_mpp_no_discount_items_list ;


-- COMMAND ----------

select count(*) from sandbox.ns_mpp_all;

-- COMMAND ----------

-- AVG PRICE ALL ITEMS
drop table if exists sandbox.ns_all_items_avg_price;
create table sandbox.ns_all_items_avg_price as

select a.product_id , avg(a.unit_price) as avg_unit_price 
from  gold.pos_transactions a inner join sandbox.ns_mpp_all b

on a.product_id = b.material_id

where (a.business_day >= '2023-12-20' and a.business_day <= '2024-01-21')
group by 1 ; 

-- COMMAND ----------

select count(*) from sandbox.ns_all_items_avg_price;

-- COMMAND ----------

drop table if exists sandbox.ns_mpp_all_with_avg_price;
create table sandbox.ns_mpp_all_with_avg_price as

select  a.* , b.avg_unit_price

from sandbox.ns_mpp_all as a 
inner join sandbox.ns_all_items_avg_price  b 

on a.material_id = b.product_id 
;


-- COMMAND ----------

-- MASTER TABLE
select * from sandbox.ns_mpp_all_with_avg_price ;

-- COMMAND ----------

-- HEALTH AND BEAUTY : 

drop table if exists sandbox.ns_hnb_promo_28_a;
create table sandbox.ns_hnb_promo_28_a as

select * 

from sandbox.ns_mpp_all_with_avg_price 

where department_name = 'HEALTH & BEAUTY' 
-- and category_name = 'FACIAL CARE'
and final_segment in ('Star' , 'Good')

and material_group_name in (
'BABY LOTIONS',
'BATH SOAPS',
'SHOWER GEL&BODY WASH',
'SKIN CARE',
'TOOTH PASTE',
'TOOTHBRUSHES',
'FACE WASH',
'FAIRNESS/WHTNG.CREAM',
'MOISTUR.CREAM/FLUID',
'CONDITIONERS',
-- 'HAIR GEL',
-- 'PERMANENT COLORANTS',
-- 'SHAMPOO',
'RUBS & BALMS',
'VITAMINS&SUPPLEMENTS',
'EAU DE PARFUM - MEN',
-- 'EAU DE PARFUM-LADIES',
-- 'EAU DE PARFUM-UNISEX',
'LIQUID HAND WASH'
'COTTON BUDS',
-- 'RAZOR DISPOSABLE',
'RAZOR SYSTEMS'
'BODY LOTION',
'FEMALE & UNISEX DEO'
'MENS DEODORANTS',
-- 'PETROLEUM JELLY',
'ROLL - ONS'
'TALCUM POWDER')


;


-- COMMAND ----------

select * from sandbox.ns_hnb_promo_28_a ; 

-- COMMAND ----------

-- HEALTH AND BEAUTY : 

drop table if exists sandbox.ns_hnb_promo_28_b;
create table sandbox.ns_hnb_promo_28_b as

select * 

from sandbox.ns_mpp_all_with_avg_price 

where department_name = 'HEALTH & BEAUTY' and category_name = 'FACIAL CARE'
and final_segment in ('Star') and avg_unit_price >= 60 
-- and material_id not in (select material_id from  sandbox.ns_hnb_promo_28_a)

-- and material_group_name in (
-- 'BATH SOAPS',
-- 'SHOWER GEL&BODY WASH',
-- 'SKIN CARE',
-- 'TOOTH PASTE',
-- 'TOOTHBRUSHES',
-- 'FACE WASH',
-- 'FAIRNESS/WHTNG.CREAM',
-- 'MOISTUR.CREAM/FLUID',
-- 'CONDITIONERS',
-- 'HAIR GEL',
-- 'PERMANENT COLORANTS',
-- 'SHAMPOO',
-- 'RUBS & BALMS',
-- 'VITAMINS&SUPPLEMENTS',
-- 'EAU DE PARFUM - MEN',
-- 'EAU DE PARFUM-LADIES',
-- 'EAU DE PARFUM-UNISEX',
-- 'COTTON BUDS',
-- 'RAZOR DISPOSABLE',
-- 'BODY LOTION',
-- 'MENS DEODORANTS',
-- 'PETROLEUM JELLY',
-- 'TALCUM POWDER',)


;


-- COMMAND ----------

select * from sandbox.ns_hnb_promo_28_b;

-- COMMAND ----------

-- HEALTH AND BEAUTY : 
-- 106

drop table if exists sandbox.ns_hnb_promo_7_a;   
create table sandbox.ns_hnb_promo_7_a as

select * from
sandbox.ns_mpp_all_with_avg_price 

where department_name = 'HEALTH & BEAUTY'
--  and category_name = 'FACIAL CARE'
and final_segment in ('Star','Good')
and material_group_name in (
'EYE PREPARATIONS' ,
'BABY SHAMPOOS',
'BABY CONDITIONERS',
'BABY BATH',
'BABY OIL',
'BABY POWDER',
'BABY COLOGNE',
'BABY COTTON NEEDS',
'BABY LOTIONS',
'BABY CREAM',
'BABY SOAP',
'OTHER BABY CARE',
'BABY FEEDING BOTTLE',
'BATH SOAPS',
'SHOWER GEL&BODY WASH',
'BATH ADDITIVES',
'BATH ACCESSORIES',
'SUN CARE',
'BODY LOTION',
'GENERAL PURPOSECREAM',
'ANTIPERSPIRANT-STICK',
'ROLL - ONS',
'MENS DEODORANTS',
'FEMALE & UNISEX DEO',
'TALCUM POWDER',
'EAU DE COLOGNE',
'OTHER SKIN CARE',
'PETROLEUM JELLY',
'MALE BODY MIST',
'FEMALE & UNISEX MIST',
'TOOTH PASTE',
'TOOTHBRUSHES',
'MOUTHWASH',
'DENTAL FLOSS',
'OTHER DENTAL CARE',
'SHAMPOO',
'CONDITIONERS',
'HAIR CREAMS',
'HAIR SPRAY',
'PERMANENT COLORANTS',
'HENNA COLORANTS',
'M/BEARD&MOSTCH CLRNT',
'HAIR OILS',
'HAIR GEL',
'HAIR TREATMENTS&MASK',
'HAIR STRAIGHTENERS',
'COUGH / THROAT DROPS',
'MUSCULAR PAIN RELIEF',
'VITAMINS&SUPPLEMENTS',
'NASAL DECONGESTANTS',
'RUBS & BALMS',
'EYE PREPARATIONS',
'ANTACID',
'ANALGESICS',
'FOOT CARE',
'LIP BALMS',
'OTHER HEALTH CARE',
'MEDICINE&FIRST AIDS.',
'ESSENTIAL OILS',
'AFTERSHAVE FRAGRANCE',
'SYSTEM BLADES',
'DOUBLE EDGE BLADES',
'RAZOR DISPOSABLE',
'RAZOR SYSTEMS',
'SHAVING PREPARE FOAM',
'SHAVING PREPARE GEL',
'SHAVING CREAM',
'SHAVING BRUSH',
'MALE HAIR REMOVERS',
'MOISTUR.CREAM/FLUID',
'EYE MAKEUP REMOVER',
'EYE CARE PRODUCT',
'FAIRNESS/WHTNG.CREAM',
'FACIAL CLEANSER',
'FACE MASK',
'FACETONER/ASTRINGENT',
'FACE WASH',
'FACIAL SCRUB',
'OTHER FACIAL CARE',
'ANTI WRINKLE',
'CONTRACEPTION-CONDOM',
'COTTON BUDS',
'COTTON WOOL',
'OTHER COTTON PRODUCT',
'LIQUID HAND WASH',
'LADIES HAIR REMOVERS',
'LADIES SHAVERS',
'HAND SANITIZER',
'EAU DE PARFUM - MEN',
'EAU DE PARFUM-LADIES',
'EAU DE PARFUM-UNISEX',
'EAU DE TOILETTE -MEN',
'EAU DETOILETE-LADIES',
'EAU DETOILETTE-UNSEX',
'PREMIUM PERFUMES',
'PERFUMES OILS',
'OTHER PERFUMES',
'EYE CARE',
'LIP CARE',
'SKIN CARE',
'NAIL CARE',
'MANICURE & PEDICURE',
'COSMETIC ACCESSORIES',
'MAKE UP SETS'
 
);


-- COMMAND ----------

select * from sandbox.ns_hnb_promo_7_a;

-- COMMAND ----------

-- GROCERY NON FOOD
-- THIS

drop table if exists sandbox.ns_gnf_promo_28_7_a;
create table sandbox.ns_gnf_promo_28_7_a as
select * from sandbox.ns_mpp_all_with_avg_price
where department_name = 'GROCERY NON FOOD' and
final_segment in ('Star','Good')
and material_group_name in (
'WASHING UP',
'DISINFECTANTS',
'TOILET CLEANERS',
'GLASS CLEANERS',
'FLOOR & CARPET CLEAN',
'INSECTICIDES',
'NAIL SAVER',
'ALL PURPOSE CLEANER',
'BATH ROOM CLEANERS',
'KITCHEN CLEANERS',
'SPECIALIST CLEANERS',
'SCOURING PADS',
'LAMINATES',
'SPIRALS',
'STEEL WOOL',
'SPONGE CLOTH',
'WASHING PWDR F.LOAD',
'WASHING PWDR T.LOAD',
'ABAYA LIQUIDS',
'FABRIC SOFTNER DILUT',
'FABRIC SOFTNERCONCNT',
'LIQUID DETERGENT',
'GARBAGE BAGS',
'KITCHEN ROLLS',
'TOILET ROLLS',
'FACIAL TISSUES',
'SANPRO PADS',
'SANPRO PANTY LINERS',
'BABY NAPPIES',
'BABY TRAINER PAN',
'BABY WIPES'



)
;


-- COMMAND ----------

select * from sandbox.ns_gnf_promo_28_7_a;

-- COMMAND ----------

-- GROCERY NON FOOD


drop table if exists sandbox.ns_gnf_promo_28_b;
create table sandbox.ns_gnf_promo_28_b as
select * from sandbox.ns_mpp_all_with_avg_price
where department_name = 'GROCERY NON FOOD' and
final_segment in ('Star')
and avg_unit_price >= 60 
-- and material_id not in (select material_id from sandbox.ns_gnf_promo_28_7_a)
and material_group_name in (


)
;


-- COMMAND ----------

select * from sandbox.ns_gnf_promo_28_b; 

-- COMMAND ----------

-- GROCERY FOOD : 28-6
-- THIS

drop table if exists sandbox.ns_gf_promo_28; 
create table sandbox.ns_gf_promo_28 as

select * from sandbox.ns_mpp_all_with_avg_price
where department_name = 'GROCERY FOOD' and
final_segment in ('Star','Good')
and material_group_name in (
'COVRD CHOCO.BARS&TAB',
'CORN FLAKES',
'COCONUT OIL',
'MINERAL /SPRING WATE',
'INDIAN SAVOURIES',
'BLACK TEA',
'CANNED MUSHROOM',
'PLAIN BISCUITS',
'PASTA',
'COFFEE',
'RUSKS',
'FRUIT DRINK TETRA',
'HONEY',
'COOKING SAUCE',
'WAFER BISCUITS',
'NUTS PROCESSED',
'SALAD DRESSINGS',
'GHEE',
'INDIAN',
'CANNED FRUIT DRINK',
'GRAPE LEAVES',
'INSTANT NOODLE',
'SAVOURY',
'FLOUR',
'CANNED TOMATOES & PUREE',
'CHOCOLATE COATED',
'HEALTH DRINKS',
'OTHER CRISPS',
'FIBER BISCUITS',
'SUNFLOWER OIL',
'POTATO CANISTER',
'KETCHUP',
'BLENDED OIL',
'BASMATI',
'BOXED CHOCOLATE',
'CANNED TUNA',
'CANOLA OIL',
'SPICES',
'ETHNICBREAKFASTPOWDR',
'POTATO BAGS',
'OTHER COOKING OIL',
'CEREAL BARS',
'CHOCOLATE BAGS',
'MUESLI',
'HEALTH CEREALS',
'MAYONNAISE'


);










-- COMMAND ----------

select * from sandbox.ns_gf_promo_28;

-- COMMAND ----------

-- GROCERY FOOD : 7-13

drop table if exists sandbox.ns_gf_promo_7;
create table sandbox.ns_gf_promo_7 as

select * from sandbox.ns_mpp_all_with_avg_price
where department_name = 'GROCERY FOOD' and
final_segment in ('Star','Good')
and material_group_name in (
'CHOCOLATE BAGS', --
'FLOUR', --
'MAYONNAISE', --
'BASMATI', --
'COCONUT OIL', --
'GHEE', --
'PLAIN BISCUITS', --
'MINERAL /SPRING WATE', --
'WHITE SUGAR', --
'COLA CAN', --
'TEA BAG', --
'CHOCOLATE DRINK',
'CORN FLAKES',
'POWDERED MILK',
'FIBER BISCUITS',
'FRUIT DRINK TETRA',
'MASALAS',
'SALT',
'BOXED CHOCOLATE',
'PASTA',
'PEANUT BUTTER',
'POTATO CANISTER',
'CORN BASED BAGS',
'BABY MILK POWDER & FORMULAS',
'SUNFLOWER OIL',
'OTHER PRESERVES & SPREADS',
'HONEY',
'CREAM FILLED BISCUIT',
'BLACK TEA',
'SHARING PACKS',
'COFFEE',
'NUTS PROCESSED',
'SPARKLING WATER',
'RUSKS',
'OLIVE OIL',
'CANNED FOUL BEANS',
'CHOCO SPREAD',
'UHT MILK',
'BASMATI')




 



;


-- COMMAND ----------

select distinct material_group_name from sandbox.ns_gf_promo_7;

-- COMMAND ----------

-- CHILLED AND DAIRY

drop table if exists sandbox.ns_cd_promo_28_7; 
--  sandbox.ns_gf_promo_28; sandbox.ns_gnf_promo_28_7_a; sandbox.ns_cd_promo_28_7; 
create table sandbox.ns_cd_promo_28_7 as

select * from sandbox.ns_mpp_all_with_avg_price where department_name = 'CHILLED & DAIRY' and
final_segment in ('Star','Good')
and material_group_name in (

'GRATED CHEESE',
'JAR CHEESE',
'PORTION CHEESE',
'SLICED CHEESE',
'DIPS ASSORTED',
'PREPARED DOUGH',
'COOKING CREAM',
'FLAVOURED YOGHURT',
'FRESH MILK',
'PLAIN YOGHURT',
'WHITE EGGS',
'CUSTARD',
'FRESH JUICE ASSORTED'


)


;


-- COMMAND ----------

select * from sandbox.ns_cd_promo_28_7;

-- COMMAND ----------

-- GROCERY FOOD : 28-6

BAB.MILKPWDR&FORMULA
TEA BAG
COLA CAN
CREAM FILLED BISCUIT
RUSKS
CORN FLAKES
OATS
CANNED TUNA
CANND FRUIT COCKTAIL
COVRD CHOCO.BARS&TAB
KIDS CHOCOLATE
SUNFLOWER OIL
GHEE
CANOLA OIL
POTATO BAGS
NUTS PROCESSED
POTATO CANISTER
FILIPINO
INDIAN
FLOUR
POWDERED MILK
UHT MILK
PASTA
JAMS
MAYONNAISE
KETCHUP
MINERAL /SPRING WATE


-- COMMAND ----------

-- GROCERY FOOD : 7-13


COVRD CHOCO.BARS&TAB
CAKE & DESSERT MIXES
SYRUPS & FROSTING
CAKE DECORATIONS
BAKING POWDER


-- COMMAND ----------

-- CHILED AND DAIRY  (JITESH) 
--  sandbox.ns_gf_promo_28; sandbox.ns_gnf_promo_28_7_a; sandbox.ns_cd_promo_28_7; 

drop table if exists sandbox.ns_cd_promo_14;
create table sandbox.ns_cd_promo_14 as

select * from sandbox.ns_cd_promo_28_7
where (lower(material_name) like '%+%' or lower(material_name) like '%x%' or lower(material_name) like '% po%')
;

-- COMMAND ----------

select * from sandbox.ns_cd_promo_14;

-- COMMAND ----------

-- GROCERRY FOOD (NISHAD) -- tbd
-- sandbox.ns_gf_promo_28; sandbox.ns_hnb_promo_7_a;   sandbox.ns_gnf_promo_28_7_a;
drop table if exists sandbox.ns_gf_promo_14;
create table sandbox.ns_gf_promo_14 as

select * from sandbox.ns_gf_promo_28
where (lower(material_name) like '%+%' or lower(material_name) like '%x%' or lower(material_name) like '% po%')
;

-- COMMAND ----------

select * from sandbox.ns_gf_promo_14;

-- COMMAND ----------

select count (distinct material_group_name) from sandbox.ns_gnf_promo_28_7_a;

-- COMMAND ----------

-- HEALTH AND BEAUTY  (RONY) 

drop table if exists sandbox.ns_hnb_promo_14;
create table sandbox.ns_hnb_promo_14 as
select * from sandbox.ns_hnb_promo_7_a
where (lower(material_name) like '%+%' or lower(material_name) like '%x%' or lower(material_name) like '% po%')
;

-- COMMAND ----------

select distinct material_group_name from  sandbox.ns_hnb_promo_14;

-- COMMAND ----------

-- GNF (SHARJU) -- same 

drop table if exists sandbox.ns_gnf_promo_14;
create table sandbox.ns_gnf_promo_14 as

select * from sandbox.ns_gnf_promo_28_7_a
where (lower(material_name) like '%+%' or lower(material_name) like '%x%' or lower(material_name) like '% po%')
;

-- COMMAND ----------

select * from sandbox.ns_gnf_promo_14;

-- COMMAND ----------

select * from sandbox.ns_gnf_promo_14; 
