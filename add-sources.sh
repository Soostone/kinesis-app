cabal sandbox init

cabal sandbox add-source $SOOSTONE_CODE_DIR/aws
cabal sandbox add-source $SOOSTONE_CODE_DIR/hedis-utils
cabal sandbox add-source $SOOSTONE_CODE_DIR/string-conv
cabal sandbox add-source $SOOSTONE_CODE_DIR/live-stats
cabal sandbox add-source $SOOSTONE_CODE_DIR/formattable
cabal sandbox add-source $SOOSTONE_CODE_DIR/hs-aws-kinesis
