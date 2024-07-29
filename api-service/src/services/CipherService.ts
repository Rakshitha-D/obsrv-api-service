import crypto from 'crypto';
import { config } from '../configs/Config';

class CipherService {
    public encrypt(data: string) {
        const cipher = crypto.createCipheriv(
            config.encryption_config.encryption_algorithm,
            config.encryption_config.encryption_key,
            "",
        )
        const toEncrypt = Buffer.from(data, 'utf8');
        let encryptedString = cipher.update(toEncrypt);
        encryptedString = Buffer.concat([encryptedString, cipher.final()])
        return encryptedString.toString('base64');
    }

    public decrypt(data: string) {
        const decipher = crypto.createDecipheriv(
            config.encryption_config.encryption_algorithm,
            config.encryption_config.encryption_key,
            "",
        )
        const encryptedText = Buffer.from(data, 'base64');
        let decryptedString = decipher.update(encryptedText);
        decryptedString = Buffer.concat([decryptedString, decipher.final()])
        return decryptedString.toString();
    }
}

export const cipherService = new CipherService()