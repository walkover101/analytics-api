import  { MAIL_REP_TABLE_ID, type } from '../../database/big-query-service'

const protoDescriptor: {name: string, field: any[]} = {name: '', field: []};
protoDescriptor.name = MAIL_REP_TABLE_ID;
protoDescriptor.field = [
  {
    name: 'requestId',
    number: 1,
    type: type.TYPE_STRING,
  },
  {
      name: 'eventId',
      number: 2,
      type: type.TYPE_INT64,
    },
    {
      name: 'statuscode',
      number: 3,
      type: type.TYPE_INT64,
    },
    {
      name: 'enhancedStatusCode',
      number: 4,
      type: type.TYPE_STRING,
    },
    {
      name: 'reason',
      number: 5,
      type: type.TYPE_STRING,
    },
    {
      name: 'resultState',
      number: 6,
      type: type.TYPE_STRING,
    },
    {
      name: 'remoteMX',
      number: 7,
      type: type.TYPE_STRING,
    },
    {
      name: 'remoteIP',
      number: 8,
      type: type.TYPE_STRING,
    },
    {
      name: 'contentSize',
      number: 9,
      type: type.TYPE_INT64,
    },
    {
      name: 'senderDedicatedIPId',
      number: 10,
      type: type.TYPE_INT64,
    },
    {
      name: 'hostname',
      number: 11,
      type: type.TYPE_STRING,
    },
    {
      name: 'recipientEmail',
      number: 12,
      type: type.TYPE_STRING,
    },
    {
      name: 'outboundEmailId',
      number: 13,
      type: type.TYPE_INT64,
    },
    {
      name: 'companyId',
      number: 14,
      type: type.TYPE_STRING,
    },
    {
      name: 'requestTime',
      number: 15,
      type: type.TYPE_STRING,

    },
    {
      name: 'createdAt',
      number: 16,
      type: type.TYPE_STRING,

    },
];

export default protoDescriptor;
