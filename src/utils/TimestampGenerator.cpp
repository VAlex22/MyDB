#include "TimestampGenerator.h"

unsigned TimestampGenerator::currentTimestamp() {
    return timestamp;
}

unsigned TimestampGenerator::nextTimestamp() {
    return ++timestamp;
}

unsigned TimestampGenerator::timestamp = 0;
