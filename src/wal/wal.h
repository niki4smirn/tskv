#pragma once
// still not sure about WAL interface, because of truncation difficulties
//
// the best idea for now is to say, that we don't store in memtable data,
// that is older, than N hours. under this assumption truncation seems to be easy 
