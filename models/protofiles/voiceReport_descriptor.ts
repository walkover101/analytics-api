import  { type } from '../../database/big-query-service'

const protoDescriptor: {name: string, field: any[]} = {name: '', field: []};
protoDescriptor.name = 'voiceReportTable';
protoDescriptor.field = [
  {
    name: 'uuid',
    number: 1,
    type: type.TYPE_STRING,
  },
  {
      name: 'companyId',
      number: 2,
      type: type.TYPE_INT64,
    },
    {
      name: 'status',
      number: 3,
      type: type.TYPE_STRING,
    },
    {
      name: 'destination',
      number: 4,
      type: type.TYPE_INT64,
    },
    {
      name: 'createdAt',
      number: 5,
      type: type.TYPE_STRING,
    },
    {
      name: 'startTime',
      number: 6,
      type: type.TYPE_STRING,
    },
    {
      name: 'endTime',
      number: 7,
      type: type.TYPE_STRING,
    },
    {
      name: 'callerId',
      number: 8,
      type: type.TYPE_INT64,
    },
    {
      name: 'duration',
      number: 9,
      type: type.TYPE_INT64,
    },
    {
      name: 'billingDuration',
      number: 10,
      type: type.TYPE_INT64,
    },
    {
      name: 'ivrInputs',
      number: 11,
      type: type.TYPE_STRING,
    },
    {
      name: 'billing',
      number: 12,
      type: type.TYPE_STRING,
    },
    {
      name: 'rate',
      number: 13,
      type: type.TYPE_STRING,
    },
    {
      name: 'charged',
      number: 14,
      type: type.TYPE_STRING,
    },
    {
      name: 'vendorBilling',
      number: 15,
      type: type.TYPE_STRING,

    },
    {
      name: 'vendorRate',
      number: 16,
      type: type.TYPE_STRING,

    },
    {
        name: 'vendorCharged',
        number: 17,
        type: type.TYPE_STRING,
  
      },
      {
        name: 'vendorRequestId',
        number: 18,
        type: type.TYPE_STRING,
  
      },
];

export default protoDescriptor;