# Question
1. housing price trend
2. fluctuation 
3. which factor affect price most?


# factors affecting price
1. location
2. size
3. school
4. crime rate
5. freeshold/leasehold
6. house type
7. age
8. flood risk
9. internet speed
10. area type


# national statistics lookup
OA11

E00000001 - E00176774 = England;
W00000001 - W00010265 = Wales;
S00088956 – S00135306 = Scotland;
N00000001 – N00004537 = Northern Ireland;
L99999999 (pseudo) = Channel Islands;
M99999999 (pseudo) = Isle of Man;



LAUA (local authority district)

E06000001 - E06000059 = England (UA);
E07000004 - E07000246 = England (LAD);
E08000001 - E08000037 = England (MD);
E09000001 - E09000033 = England (LB);
W06000001 - W06000024 = Wales (UA);
S12000005 - S12000050 = Scotland (CA);


park

E26000001 - E26000012 = England;
E99999999 (pseudo) = England (non-National Park);
W18000001 - W18000003 = Wales;
W31000001 = Wales (non-National Park);
S21000002 - S21000003 = Scotland;
S99999999 (pseudo) = Scotland (non-National Park);


RU11IND

A1	(England/Wales) Urban major conurbation
B1	(England/Wales) Urban minor conurbation
C1	(England/Wales) Urban city and town
C2	(England/Wales) Urban city and town in a sparse setting
D1	(England/Wales) Rural town and fringe
D2	(England/Wales) Rural town and fringe in a sparse setting
E1	(England/Wales) Rural village
E2	(England/Wales) Rural village in a sparse setting
F1	(England/Wales) Rural hamlet and isolated dwellings
F2	(England/Wales) Rural hamlet and isolated dwellings in a sparse setting
1	(Scotland) Large Urban Area
2	(Scotland) Other Urban Area
3	(Scotland) Accessible Small Town
4	(Scotland) Remote Small Town
5	(Scotland) Very Remote Small Town
6	(Scotland) Accessible Rural
7	(Scotland) Remote Rural
8	(Scotland) Very Remote Rural
Z9	(pseudo) Channel Islands/Isle of Man




('A1','B1','C1','C2','D1','D2','E1','E2','F1','F2','1','2','3','4','5','6','7','8','Z9')



# PPD
Transaction unique identifier 	A reference number which is generated automatically recording each published sale. The number is unique and will change each time a sale is recorded.

Price 	Sale price stated on the transfer deed.

Date of Transfer 	Date when the sale was completed, as stated on the transfer deed.

Postcode 	This is the postcode used at the time of the original transaction. Note that postcodes can be reallocated and these changes are not reflected in the Price Paid Dataset.

Property Type 	D = Detached, S = Semi-Detached, T = Terraced, F = Flats/Maisonettes, O = Other

Note that:
- we only record the above categories to describe property type, we do not separately identify bungalows.
- end-of-terrace properties are included in the Terraced category above.
- ‘Other’ is only valid where the transaction relates to a property type that is not covered by existing values.

Old/New 	Indicates the age of the property and applies to all price paid transactions, residential and non-residential.
Y = a newly built property, N = an established residential building

Duration 	Relates to the tenure: F = Freehold, L= Leasehold etc.

PAON 	Primary Addressable Object Name. Typically the house number or name.

SAON 	Secondary Addressable Object Name. Where a property has been divided into separate units (for example, flats), the PAON (above) will identify the building and a SAON will be specified that identifies the separate unit/flat.

Street 	 

Locality 	 

Town/City 	 

District 	 

County 	 

PPD Category Type 	Indicates the type of Price Paid transaction.
A = Standard Price Paid entry, includes single residential property sold for value.
B = Additional Price Paid entry including transfers under a power of sale/repossessions, buy-to-lets (where they can be identified by a Mortgage) and transfers to non-private individuals.

Note that category B does not separately identify the transaction types stated.
HM Land Registry has been collecting information on Category A transactions from January 1995. Category B transactions were identified from October 2013.
Record Status - monthly file only 	Indicates additions, changes and deletions to the records.(see guide below).
A = Addition
C = Change
D = Delete.




