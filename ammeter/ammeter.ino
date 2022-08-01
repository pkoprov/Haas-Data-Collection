int inpPin1 = 36;
int inpPin2 = 39;
int inpPin3 = 34;              //Assign CT inputs
int sampleI1, sampleI2, sampleI3;     //sample_ holds the raw analog read value
int ADC_BITS = 12;          // # of bits of ADC               
double sumI1, sumI2, sumI3;              //sq = squared, sum = Sum
double filteredI1, filteredI2, filteredI3;          //Filtered_ is the raw analog value minus the DC offset
int SupplyVoltage=3300;    //mVolts 
int Number_of_Samples = 2000;  
double Irms[3]; 
double ICAL = 100.0;  // max I value that CT can measure

int ADC_COUNTS   = 1<<ADC_BITS; 
double I_RATIO = ICAL *((SupplyVoltage/1000.0) / (ADC_COUNTS));
double offsetI1 = ADC_COUNTS>>1, offsetI2 = ADC_COUNTS>>1, offsetI3 =  ADC_COUNTS>>1; //Low-pass filter output


void setup() {
  Serial.begin(115200);
  Serial.println("Running");
}


void loop() {
    
    for (unsigned int n = 0; n < Number_of_Samples; n++)
    {
      sampleI1 = analogRead(inpPin1);
      sampleI2 = analogRead(inpPin2);
      sampleI3 = analogRead(inpPin3);
      // Digital low pass filter extracts the 1.65 V dc offset,
      // then subtract this - signal is now centered on 0 counts.
      offsetI1 = (offsetI1 + (sampleI1 - offsetI1)/ADC_COUNTS);
      offsetI2 = (offsetI2 + (sampleI2 - offsetI2)/ADC_COUNTS);
      offsetI3 = (offsetI3 + (sampleI3 - offsetI3)/ADC_COUNTS);
      filteredI1 = sampleI1 - offsetI1;
      filteredI2 = sampleI2 - offsetI2;
      filteredI3 = sampleI3 - offsetI3;
      // Root-mean-square method current
      sumI1 += filteredI1*filteredI1;
      sumI2 += filteredI2*filteredI2;
      sumI3 += filteredI3*filteredI3;
      }

    Irms[0] = I_RATIO * sqrt(sumI1 / Number_of_Samples) - 0.45;
    Irms[1] = I_RATIO * sqrt(sumI2 / Number_of_Samples) - 0.45;
    Irms[2] = I_RATIO * sqrt(sumI3 / Number_of_Samples) - 0.45;
    //Reset accumulators
    sumI1 = sumI2 = sumI3 = 0; 

//  Serial.print(String("Irms for phases [1, 2, 3] is: "));
  int i  = 0;
  while  (i < 3){
    Serial.print(Irms[i]);
    i++;
    if (i > 2) {break;}
    Serial.print(String(", "));
    }
  Serial.println();
//  Serial.println(String("***********************"));
}
