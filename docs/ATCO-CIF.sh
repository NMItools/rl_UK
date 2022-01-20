# QS - Bus journey header
# N - Transaction Type - NEW
# 319 - Short code form of operator ID
# 020S3_ - Unique ID of a journey with OP
# 20110104 - Start date of operation of journey (yyyymmdd)
# 21991231 - Last date of operation of journey
# 1 - Operates on Modays (1/0 true/false)
# 1 - Tue
# 1 - Wed
# 1 - Thur
# 1 - Fri
# 0 - Sat
# 0 - Sun
#   - School term time
#   - Bank Holidays
# other useless shit...untill
# S3 - Route Number ID
# LFBUS - Vehicle Type
# 0 - Route Direction (wat?)
QSN319 020S3_20110104219912311111100  S3        LFBUS           O

# QE - Journey Date Running identifier
# 20120604 - Start of the period
# 20120604 - End of the period
# 0 - Operation code, dodes not operate between dates, 1 - does operate
QE20120604201206040
QE20120605201206050
QE20120827201208270

# QO - Bus Journey Origin identifier
# 340000006R5 - Short code form of origin location (atco code in the db)
# 0655 - Public departure time (from the depo?)
# __ - Bay/Stop identifier
# T1/T0 - Timing/Non timing point
# F1/F0 - Fare stage/not fare stage (not applicable for busses)
QO340000006R5 0655   T1

# QI - Bus Journey Inermediate id
# 34000000008 - Short code form of indermed location (atco code in the database)
# 0700 - public arrival time
# 0705 - public departure time - certain bays and stops, for most normal stops will be the same as arr time
# B - Activity type. P=pick up, S=set down, B=both, N=neither.
# xxx - Bay/Stop identifier
# T1/T0 - Timing/non timing
QI34000000008 07000705B   T1  
QI340000864B2 07050705B   T0  
QI340001002OUT07070707B   T0  
QI340001001CNR07070707B   T0  
QI340001000WR107080708B   T0  
QI340000999OUT07080708B   T0  
QI340000998WR407090709B   T0  
QI340000997OPP07100710B   T0  
QI340000996OPP07110711B   T1  
QI340000995CNR07110711B   T0  
QI340000994CNR07120712B   T0  
QI340000993OPP07120712B   T0  
QI340000992WR207120712B   T0  
QI340003247OPP07140714B   T0  
QI340001864OPG07170717B   T0  
QI340001865OPP07190719B   T1  
QI340001536STO07190719B   T0  
QI340001538NTH07190719B   T0  
QI340001535OPP07200720B   T0  
QI340003246OPP07210721B   T0  
QI340001534OUT07230723B   T1  
QI340001866W  07240724B   T0  
QI340000924N  07260726B   T0  
QI340001004OUT07290729B   T1  
QI340001867OPP07300730B   T1  
QI340001868OPP07320732B   T0  

# QT - Journey Destination
# 340000595VD - Short code form of dest location
# 0733 - Public arrival time
# T1 - Timinig point
QT340000595VD 0733   T1
