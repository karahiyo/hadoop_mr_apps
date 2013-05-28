#!/usr/bin/ruby

currentKey = nil
total = 0

STDIN.each_line do |line|
  key, value = line.split("\t")
  value = value.to_i
  if currentKey && currentKey != key
    printf("%s\t%d\n",currentKey,total)
    currentKey = key
    total = value
  else
    currentKey = key
    total = total + value
  end
end
printf("%s\t%d\n",currentKey,total)
