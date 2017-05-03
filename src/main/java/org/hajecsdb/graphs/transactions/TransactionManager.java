package org.hajecsdb.graphs.transactions;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

public class TransactionManager {
    private int defaultTimeout;
    private int maximumTimeout;

    // zazadza transakcjami
    // poziom izolacji transakcji

    public Transaction createTransaction() {
        return new Transaction() {
            private long transactionId = generateUniqueId();
            private boolean performed = false;
            private TransactionStatus transactionStatus = new TransactionStatus();
            private TransactionScope transactionScope = new TransactionScope();

            @Override
            public long getId() {
                return transactionId;
            }

            @Override
            public TransactionStatus commit() {
                performed = true;
                return null;
            }

            @Override
            public TransactionStatus rollback() {
                performed = true;
                return null;
            }

            @Override
            public TransactionStatus getStatus() {
                return transactionStatus;
            }

            @Override
            public TransactionScope getScope() {
                return transactionScope;
            }

            @Override
            public boolean isPerformed() {
                return performed;
            }


            /**
             * Gnereate unique ID from UUID in positive space
             * @return long value representing UUID
             */
            private Long generateUniqueId()
            {
                long val = -1;
                do
                {
                    final UUID uid = UUID.randomUUID();
                    final ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
                    buffer.putLong(uid.getLeastSignificantBits());
                    buffer.putLong(uid.getMostSignificantBits());
                    final BigInteger bi = new BigInteger(buffer.array());
                    val = bi.longValue();
                } while (val < 0);
                return val;
            }
        };
    }
}
