#rm -rf ./gen-cpp
/data/data/zserver/thrift9.3/bin/thrift -gen go -r ./bigsetlistint.thrift
/data/data/zserver/thrift9.3/bin/thrift -gen go -r bigsetgenericdata.thrift
/data/data/zserver/thrift9.3/bin/thrift -gen go  -r ./idgen.thrift
#name=bigsetlistint
##sed -i '/virtual ~/d' gen-cpp/"$name"_types.h
#sed -i  's/virtual ~/~/' gen-cpp/"$name"_types.h

##sed -i '/::~/,3d' gen-cpp/"$name"_types.cpp
