import AWS from "aws-sdk"
import fs from "fs"
import * as dotenv from "dotenv"

dotenv.config()

// AWSの設定
AWS.config.update({
    accessKeyId: process.env.ACCESS_KEY_ID as string,
    secretAccessKey: process.env.SECRET_ACCESS_KEY as string,
})

// S3のバケット名とディレクトリパス
const bucketName = process.env.S3_BUCKET as string
const prefix = "tdnet/pdf/20210528"

// S3オブジェクトを生成
const s3 = new AWS.S3()

// ファイルの一覧を取得
const listObjects = async (): Promise<AWS.S3.ObjectList> => {
    const params: AWS.S3.ListObjectsV2Request = {
        Bucket: bucketName,
        Prefix: prefix,
    }

    const data = await s3.listObjectsV2(params).promise()

    if (data.Contents === undefined) {
        throw Error("Contents is undefined")
    }
    return data.Contents
}

// ファイルをダウンロード
const downloadFile = async (key: string): Promise<string> => {
    const params: AWS.S3.GetObjectRequest = {
        Bucket: bucketName,
        Key: key,
    }

    const splited = key.split("/")
    const dataPath = splited[splited.length - 2]
    const filename = splited[splited.length - 1]
    const dirPath = `data/pdf/${dataPath}`
    if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, { recursive: true })
    }

    const data = await s3.getObject(params).promise()
    const storePath = `${dirPath}/${filename}`
    fs.writeFileSync(storePath, data.Body as string)

    return key
}

// メイン処理
;(async () => {
    const objects = await listObjects()

    const downloadPromises = objects.map((object) => {
        return downloadFile(object.Key || "")
    })

    const results = await Promise.allSettled(downloadPromises)
    results.forEach((result) => {
        switch (result.status) {
            case "fulfilled":
                console.log(`${result.status}: ${result.value}`)
            case "rejected":
                console.log(
                    `${result.status}: ${
                        (result as PromiseRejectedResult).reason
                    }`
                )
        }
    })

    console.log(`Downloaded ${objects.length} objects.`)
})()
