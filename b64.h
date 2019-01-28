/* Base64 Encoding Library (c)2013 Justin Jack */

unsigned char b64Chart[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
struct b64input {
	unsigned char two:2;
	unsigned char one:6;
	unsigned char four:4;
	unsigned char three:4;
	unsigned char six:6;
	unsigned char five:2;
};

int base64encode(unsigned char *input_string, int sizein, char **lpszoutput)
/*
	unsigned char *input_string   ---- cannot be a NULL pointer
	int sizein                    ---- cannot be zero
	char **lpszoutput             ---- must be a pointer to a NULL pointer.
								       the pointer that is returned must be
								       freed after use. (AND zero'd to use it
								       again)

	Function returns the number of bytes in the Base64 Encoded string on success,
	or -1 on failure.


*/
{
	int bytesEncoded = 0, blocks=0, rmndr = 0, i=0, j=0;
	unsigned char *input = 0;
	struct b64input *inputchars=0;
	if ( (((*lpszoutput) || (!input_string)) || (!sizein)) )
		return -1;
	blocks = sizein / 3;
	rmndr = sizein % 3;
	bytesEncoded = ((blocks*4) + 4);
	input = (unsigned char *)malloc(sizein+rmndr+1);
	for(i=0; i<(sizein+rmndr+1); i++)
		input[i] = 0;
	for(i=0; i<sizein; i++)
		input[i]=input_string[i];

	if (!rmndr) {
		bytesEncoded-=4;
	} else {
		blocks++;
	}
	*lpszoutput = (char *) malloc(bytesEncoded + 1 + (rmndr?((rmndr==1)?2:1):0));
	for (i=0; i<=bytesEncoded; i++)
		(*lpszoutput)[i] = 0;
	j = 0;
	for (i=0; i<=((blocks*3)-3  );i+=3) {
		inputchars = (struct b64input *) (input+i);
		((*lpszoutput)+j)[0] = b64Chart[inputchars->one];
		((*lpszoutput)+j)[1] = b64Chart[((inputchars->two << 4) | inputchars->three)];
		((*lpszoutput)+j)[2] = b64Chart[((inputchars->four << 2) | inputchars->five)];
		((*lpszoutput)+j)[3] = b64Chart[inputchars->six];
		j+=4;
	}
	(*lpszoutput)[bytesEncoded]=0;
	for (i = (bytesEncoded-1); i > (bytesEncoded-1)-(3-(rmndr?((rmndr==1)?1:2):3)); i--)
		(*lpszoutput)[i] = '=';
	free(input);
	return bytesEncoded;
}


int base64decode(unsigned char *input, int length, char **outbuf)
/*
 * Arguments:
 *
 * unsigned char *input        ----- pointer to the data to be decoded
 * int length                  ----- the length of the data pointed to by input
 * char **outbuf               ----- pointer to a pointer variable to receive the
 *                             ----- decoded data.  The pointer you allocate must be
 *                             ----- NULL.  After this call, you must free() the data
 *                             ----- yourself, and ZERO the pointer again before it's
 *                             ----- reuse.  If the pointer is not NULL, the function
 *                             ----- will fail with error code -1.
 *
 * Returns an error (-1) if:
 * unsigned char *input is NULL (0)
 * int length is equal to zero, less than four OR not EVENLY divisible by FOUR.
 * char **outbuf is NOT NULL
 *
 *
 */

{
	int i=0, j=0, k=0, blocks=0, byteswritten=0, bufferskipped = 0;
	if ( ((((!input) || (length<4)) || (*outbuf)) || (length%4)) )
		return -1;
	*outbuf = (char *)malloc(length+1);
	blocks = length/4;
	for (i=0; i<((blocks*4)-0); i+=4) {
		for (j=i; j<i+4; j++) {
			if (input[j] == '='){
				bufferskipped++;
				k = 0;
			} else {
				for (k=0; k<64; k++)
					if (input[j] == b64Chart[k])
						break;
			}
			switch (j-i) {
			case 0:
				(*outbuf)[byteswritten] = ((k&0x3f) << 2);
				break;
			case 1:
				(*outbuf)[byteswritten++] |= ((k&0x30) >> 4);
				(*outbuf)[byteswritten] = ((k&0xf)<<4);
				break;
			case 2:
				(*outbuf)[byteswritten++] |= ((k&0x3c)>>2);
				(*outbuf)[byteswritten] = ((k&0x3) << 6);
				break;
			case 3:
				(*outbuf)[byteswritten++] |= (k&0x3f);
				break;
			}
		}
	}
	byteswritten-=bufferskipped;
	(*outbuf)[byteswritten] = 0;
	return byteswritten;
}
