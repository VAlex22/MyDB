#ifndef MYDB_TIMESTAMPGENERATOR_H
#define MYDB_TIMESTAMPGENERATOR_H


class TimestampGenerator {
public:
    static unsigned currentTimestamp();
    static unsigned nextTimestamp();

private:
    static unsigned timestamp;

};


#endif //MYDB_TIMESTAMPGENERATOR_H
