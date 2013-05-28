#!/usr/bin/ruby

STDIN.each_line do |line|
  val = line
  words = val.split(" ")
  for w in words
    printf("%s\t%d\n",w,1)
  end
end
