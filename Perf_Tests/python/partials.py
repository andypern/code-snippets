from functools import partial

def power(base, exponent):
	print "%s , %s" %(base,exponent)

	return base ** exponent


def test_partials():
	print "main"
	assert square(2) == 4
	assert cube(2) == 8

square = partial(power, exponent=2)
cube = partial(power, exponent=3)


test_partials()
